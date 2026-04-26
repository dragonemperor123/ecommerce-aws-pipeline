import os
import shutil
import subprocess
import jsii
from aws_cdk import (
    Stack,
    Duration,
    BundlingOptions,
    ILocalBundling,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_es,
    aws_iam as iam,
    aws_glue as glue,
    aws_athena as athena,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct
from .storage_stack import StorageStack
from .ingestion_stack import IngestionStack
from .notification_stack import NotificationStack


@jsii.implements(ILocalBundling)
class LocalBundler:
    """Bundles a Lambda by running pip install locally — no Docker needed."""
    def __init__(self, source_path: str):
        self.source_path = os.path.abspath(source_path)

    def try_bundle(self, output_dir: str, options: BundlingOptions) -> bool:
        try:
            req = os.path.join(self.source_path, "requirements.txt")
            if os.path.exists(req):
                subprocess.run(
                    ["pip", "install", "-r", req, "-t", output_dir, "-q"],
                    check=True,
                )
            for item in os.listdir(self.source_path):
                s = os.path.join(self.source_path, item)
                d = os.path.join(output_dir, item)
                if os.path.isdir(s):
                    shutil.copytree(s, d, dirs_exist_ok=True)
                else:
                    shutil.copy2(s, d)
            return True
        except Exception as e:
            print(f"Local bundling failed for {self.source_path}: {e}")
            return False


class ProcessingStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        storage_stack: StorageStack,
        ingestion_stack: IngestionStack,
        notification_stack: NotificationStack,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        lambda_role = iam.Role(
            self, "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ],
        )
        storage_stack.raw_bucket.grant_read_write(lambda_role)
        storage_stack.processed_bucket.grant_read_write(lambda_role)
        storage_stack.orders_table.grant_read_write_data(lambda_role)
        storage_stack.products_table.grant_read_write_data(lambda_role)
        storage_stack.sessions_table.grant_read_write_data(lambda_role)
        ingestion_stack.orders_queue.grant_consume_messages(lambda_role)
        ingestion_stack.clickstream_queue.grant_consume_messages(lambda_role)
        ingestion_stack.inventory_queue.grant_consume_messages(lambda_role)
        notification_stack.fraud_topic.grant_publish(lambda_role)
        notification_stack.order_events_topic.grant_publish(lambda_role)
        notification_stack.low_inventory_topic.grant_publish(lambda_role)

        def bundled_asset(path: str) -> _lambda.Code:
            abs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", path))
            return _lambda.Code.from_asset(
                abs_path,
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    local=LocalBundler(abs_path),
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -r . /asset-output",
                    ],
                ),
            )

        common_env = {
            "ORDERS_TABLE": storage_stack.orders_table.table_name,
            "PRODUCTS_TABLE": storage_stack.products_table.table_name,
            "SESSIONS_TABLE": storage_stack.sessions_table.table_name,
            "RAW_BUCKET": storage_stack.raw_bucket.bucket_name,
            "PROCESSED_BUCKET": storage_stack.processed_bucket.bucket_name,
            "FRAUD_TOPIC_ARN": notification_stack.fraud_topic.topic_arn,
            "ORDER_EVENTS_TOPIC_ARN": notification_stack.order_events_topic.topic_arn,
            "LOW_INVENTORY_TOPIC_ARN": notification_stack.low_inventory_topic.topic_arn,
        }

        # ── Lambda: Order Processor ───────────────────────────────────────────
        self.order_processor = _lambda.Function(
            self, "OrderProcessor",
            function_name="ecom-order-processor",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.lambda_handler",
            code=bundled_asset("../lambdas/order_processor"),
            role=lambda_role,
            timeout=Duration.seconds(60),
            memory_size=512,
            environment=common_env,
        )
        self.order_processor.add_event_source(
            lambda_es.SqsEventSource(
                ingestion_stack.orders_queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )

        # ── Lambda: Stream Enricher (clickstream) ─────────────────────────────
        self.stream_enricher = _lambda.Function(
            self, "StreamEnricher",
            function_name="ecom-stream-enricher",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.lambda_handler",
            code=bundled_asset("../lambdas/stream_enricher"),
            role=lambda_role,
            timeout=Duration.seconds(60),
            memory_size=256,
            environment=common_env,
        )
        self.stream_enricher.add_event_source(
            lambda_es.SqsEventSource(
                ingestion_stack.clickstream_queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )

        # ── Lambda: Fraud Detector ────────────────────────────────────────────
        notification_stack.fraud_queue.grant_consume_messages(lambda_role)

        self.fraud_detector = _lambda.Function(
            self, "FraudDetector",
            function_name="ecom-fraud-detector",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.lambda_handler",
            code=bundled_asset("../lambdas/fraud_detector"),
            role=lambda_role,
            timeout=Duration.seconds(30),
            memory_size=256,
            environment=common_env,
        )
        # Triggered by SQS queue that subscribes to the fraud SNS topic
        self.fraud_detector.add_event_source(
            lambda_es.SqsEventSource(
                notification_stack.fraud_queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )

        # ── Lambda: Inventory Alerter ─────────────────────────────────────────
        self.inventory_alerter = _lambda.Function(
            self, "InventoryAlerter",
            function_name="ecom-inventory-alerter",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.lambda_handler",
            code=bundled_asset("../lambdas/inventory_alerter"),
            role=lambda_role,
            timeout=Duration.seconds(30),
            memory_size=128,
            environment=common_env,
        )
        self.inventory_alerter.add_event_source(
            lambda_es.SqsEventSource(
                ingestion_stack.inventory_queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )

        # ── Glue Database ─────────────────────────────────────────────────────
        self.glue_db = glue.CfnDatabase(
            self, "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="ecom_datalake",
                description="Ecommerce data lake — orders, sessions, products",
            ),
        )

        glue_role = iam.Role(
            self, "GlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
            ],
        )
        storage_stack.raw_bucket.grant_read(glue_role)
        storage_stack.processed_bucket.grant_read_write(glue_role)
        storage_stack.curated_bucket.grant_read_write(glue_role)

        # ── Glue Crawler (raw zone) ───────────────────────────────────────────
        self.raw_crawler = glue.CfnCrawler(
            self, "RawCrawler",
            name="ecom-raw-crawler",
            role=glue_role.role_arn,
            database_name="ecom_datalake",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(path=f"s3://{storage_stack.raw_bucket.bucket_name}/"),
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression="cron(0 * * * ? *)"),
            configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
        )

        # ── Glue Crawler (processed zone) ────────────────────────────────────
        self.processed_crawler = glue.CfnCrawler(
            self, "ProcessedCrawler",
            name="ecom-processed-crawler",
            role=glue_role.role_arn,
            database_name="ecom_datalake",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(path=f"s3://{storage_stack.processed_bucket.bucket_name}/"),
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression="cron(30 * * * ? *)"),
            configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
        )

        # ── Glue Crawler (curated zone) — discovers daily_kpis, customer_clv,
        #    product_interactions, state_revenue for Athena ───────────────────
        self.curated_crawler = glue.CfnCrawler(
            self, "CuratedCrawler",
            name="ecom-curated-crawler",
            role=glue_role.role_arn,
            database_name="ecom_datalake",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(path=f"s3://{storage_stack.curated_bucket.bucket_name}/"),
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression="cron(45 * * * ? *)"),
            configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
        )

        # ── Glue ETL Job: Raw → Processed ─────────────────────────────────────
        self.etl_job = glue.CfnJob(
            self, "ETLJob",
            name="ecom-raw-to-processed",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{storage_stack.processed_bucket.bucket_name}/glue-scripts/raw_to_processed.py",
            ),
            glue_version="4.0",
            number_of_workers=2,
            worker_type="G.1X",
            default_arguments={
                "--job-language": "python",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--RAW_BUCKET": storage_stack.raw_bucket.bucket_name,
                "--PROCESSED_BUCKET": storage_stack.processed_bucket.bucket_name,
                "--CURATED_BUCKET": storage_stack.curated_bucket.bucket_name,
            },
        )

        # ── Athena Workgroup ──────────────────────────────────────────────────
        self.athena_workgroup = athena.CfnWorkGroup(
            self, "AthenaWorkgroup",
            name="ecom-analytics",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{storage_stack.curated_bucket.bucket_name}/athena-results/",
                ),
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                bytes_scanned_cutoff_per_query=1_000_000_000,
            ),
        )
