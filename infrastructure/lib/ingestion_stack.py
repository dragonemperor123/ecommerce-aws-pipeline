from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    aws_sqs as sqs,
)
from constructs import Construct
from .storage_stack import StorageStack


class IngestionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, storage_stack: StorageStack, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ── SQS Queues (replacing Kinesis — available on all account types) ──
        self.orders_queue = sqs.Queue(
            self, "OrdersQueue",
            queue_name="ecom-orders-queue",
            visibility_timeout=Duration.seconds(300),
            retention_period=Duration.hours(24),
        )

        self.clickstream_queue = sqs.Queue(
            self, "ClickStreamQueue",
            queue_name="ecom-clickstream-queue",
            visibility_timeout=Duration.seconds(300),
            retention_period=Duration.hours(24),
        )

        self.inventory_queue = sqs.Queue(
            self, "InventoryQueue",
            queue_name="ecom-inventory-queue",
            visibility_timeout=Duration.seconds(300),
            retention_period=Duration.hours(24),
        )

        # Export queue URLs so the generator script can read them
        CfnOutput(self, "OrdersQueueUrl",      value=self.orders_queue.queue_url)
        CfnOutput(self, "ClickStreamQueueUrl", value=self.clickstream_queue.queue_url)
        CfnOutput(self, "InventoryQueueUrl",   value=self.inventory_queue.queue_url)
        CfnOutput(self, "OrdersQueueName",      value=self.orders_queue.queue_name)
        CfnOutput(self, "ClickStreamQueueName", value=self.clickstream_queue.queue_name)
        CfnOutput(self, "InventoryQueueName",   value=self.inventory_queue.queue_name)
