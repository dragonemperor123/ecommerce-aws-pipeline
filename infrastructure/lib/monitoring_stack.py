from aws_cdk import (
    Stack,
    Duration,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
)
from constructs import Construct
from .ingestion_stack import IngestionStack
from .processing_stack import ProcessingStack


class MonitoringStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        ingestion_stack: IngestionStack,
        processing_stack: ProcessingStack,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        ops_topic = sns.Topic(self, "OpsTopic", topic_name="ecom-ops-alerts")

        # ── SQS Queue Depth Metrics ───────────────────────────────────────────
        orders_queue_depth = cw.Metric(
            namespace="AWS/SQS",
            metric_name="ApproximateNumberOfMessagesVisible",
            dimensions_map={"QueueName": ingestion_stack.orders_queue.queue_name},
            statistic="Maximum",
            period=Duration.minutes(1),
        )

        # ── Lambda Error Alarms ───────────────────────────────────────────────
        for fn_name, fn in [
            ("OrderProcessor",    processing_stack.order_processor),
            ("StreamEnricher",    processing_stack.stream_enricher),
            ("FraudDetector",     processing_stack.fraud_detector),
            ("InventoryAlerter",  processing_stack.inventory_alerter),
        ]:
            alarm = cw.Alarm(
                self, f"{fn_name}ErrorAlarm",
                alarm_name=f"ecom-{fn_name.lower()}-errors",
                metric=fn.metric_errors(period=Duration.minutes(5)),
                threshold=5,
                evaluation_periods=1,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                alarm_description=f"{fn_name} Lambda error rate > 5 in 5 min",
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            )
            alarm.add_alarm_action(cw_actions.SnsAction(ops_topic))

        # ── Queue Depth Alarm (pipeline falling behind) ───────────────────────
        lag_alarm = cw.Alarm(
            self, "OrderQueueDepthAlarm",
            alarm_name="ecom-orders-queue-depth",
            metric=orders_queue_depth,
            threshold=1000,
            evaluation_periods=3,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            alarm_description="Orders queue depth > 1000 messages — consumer may be lagging",
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        lag_alarm.add_alarm_action(cw_actions.SnsAction(ops_topic))

        # ── CloudWatch Dashboard ──────────────────────────────────────────────
        self.dashboard = cw.Dashboard(
            self, "EcomDashboard",
            dashboard_name="EcommerceDataPipeline",
        )

        self.dashboard.add_widgets(
            cw.GraphWidget(
                title="Orders Queue Depth (messages)",
                left=[orders_queue_depth],
                width=12,
            ),
            cw.GraphWidget(
                title="Lambda Invocations",
                left=[
                    processing_stack.order_processor.metric_invocations(period=Duration.minutes(1)),
                    processing_stack.stream_enricher.metric_invocations(period=Duration.minutes(1)),
                    processing_stack.fraud_detector.metric_invocations(period=Duration.minutes(1)),
                    processing_stack.inventory_alerter.metric_invocations(period=Duration.minutes(1)),
                ],
                width=12,
            ),
            cw.GraphWidget(
                title="Lambda Errors",
                left=[
                    processing_stack.order_processor.metric_errors(period=Duration.minutes(5)),
                    processing_stack.stream_enricher.metric_errors(period=Duration.minutes(5)),
                    processing_stack.fraud_detector.metric_errors(period=Duration.minutes(5)),
                    processing_stack.inventory_alerter.metric_errors(period=Duration.minutes(5)),
                ],
                width=12,
            ),
            cw.GraphWidget(
                title="Lambda Duration (ms)",
                left=[
                    processing_stack.order_processor.metric_duration(period=Duration.minutes(1)),
                    processing_stack.stream_enricher.metric_duration(period=Duration.minutes(1)),
                    processing_stack.fraud_detector.metric_duration(period=Duration.minutes(1)),
                    processing_stack.inventory_alerter.metric_duration(period=Duration.minutes(1)),
                ],
                width=12,
            ),
        )
