from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    CfnOutput,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
)
from constructs import Construct


class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ── S3 Data Lake ─────────────────────────────────────────────────────
        lifecycle_rules = [
            s3.LifecycleRule(
                id="archive-after-90-days",
                transitions=[
                    s3.Transition(
                        storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                        transition_after=Duration.days(90),
                    )
                ],
            )
        ]

        self.raw_bucket = s3.Bucket(
            self, "RawBucket",
            bucket_name=None,  # auto-generate
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=lifecycle_rules,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        self.processed_bucket = s3.Bucket(
            self, "ProcessedBucket",
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=lifecycle_rules,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        self.curated_bucket = s3.Bucket(
            self, "CuratedBucket",
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        self.model_bucket = s3.Bucket(
            self, "ModelBucket",
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # ── DynamoDB Tables ───────────────────────────────────────────────────
        self.orders_table = dynamodb.Table(
            self, "OrdersTable",
            partition_key=dynamodb.Attribute(name="order_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="customer_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            point_in_time_recovery=True,
        )
        self.orders_table.add_global_secondary_index(
            index_name="customer-index",
            partition_key=dynamodb.Attribute(name="customer_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="created_at", type=dynamodb.AttributeType.STRING),
        )

        self.products_table = dynamodb.Table(
            self, "ProductsTable",
            partition_key=dynamodb.Attribute(name="product_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True,
        )

        self.sessions_table = dynamodb.Table(
            self, "SessionsTable",
            partition_key=dynamodb.Attribute(name="session_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            time_to_live_attribute="expires_at",
        )

        # ── Stack Outputs (consumed by deploy.sh and the analytics notebook) ──
        CfnOutput(self, "RawBucketName",       value=self.raw_bucket.bucket_name)
        CfnOutput(self, "ProcessedBucketName", value=self.processed_bucket.bucket_name)
        CfnOutput(self, "CuratedBucketName",   value=self.curated_bucket.bucket_name)
        CfnOutput(self, "ModelBucketName",     value=self.model_bucket.bucket_name)
        CfnOutput(self, "OrdersTableName",     value=self.orders_table.table_name)
        CfnOutput(self, "ProductsTableName",   value=self.products_table.table_name)
