from aws_cdk import (
    Stack,
    Duration,
    aws_kinesis as kinesis,
    aws_firehose as firehose,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct
from .storage_stack import StorageStack


class IngestionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, storage_stack: StorageStack, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ── Kinesis Data Streams ──────────────────────────────────────────────
        self.orders_stream = kinesis.Stream(
            self, "OrdersStream",
            stream_name="ecom-orders-stream",
            shard_count=2,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.MANAGED,
        )

        self.clickstream = kinesis.Stream(
            self, "ClickStream",
            stream_name="ecom-clickstream",
            shard_count=4,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.MANAGED,
        )

        self.inventory_stream = kinesis.Stream(
            self, "InventoryStream",
            stream_name="ecom-inventory-stream",
            shard_count=1,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.MANAGED,
        )

        # ── Kinesis Firehose → S3 Raw (clickstream backup) ────────────────────
        firehose_role = iam.Role(
            self, "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )
        storage_stack.raw_bucket.grant_read_write(firehose_role)

        self.clickstream_firehose = firehose.CfnDeliveryStream(
            self, "ClickstreamFirehose",
            delivery_stream_name="ecom-clickstream-firehose",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                kinesis_stream_arn=self.clickstream.stream_arn,
                role_arn=firehose_role.role_arn,
            ),
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=storage_stack.raw_bucket.bucket_arn,
                role_arn=firehose_role.role_arn,
                prefix="clickstream/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/",
                error_output_prefix="clickstream-errors/",
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    size_in_m_bs=64,
                    interval_in_seconds=60,
                ),
                compression_format="GZIP",
                data_format_conversion_configuration=firehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
                    enabled=False,
                ),
            ),
        )
