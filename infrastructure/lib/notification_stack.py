from aws_cdk import (
    Stack,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_sqs as sqs,
    Duration,
)
from constructs import Construct


class NotificationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ── SNS Topics ────────────────────────────────────────────────────────
        self.fraud_topic = sns.Topic(
            self, "FraudAlertTopic",
            topic_name="ecom-fraud-alerts",
            display_name="Ecommerce Fraud Alerts",
        )

        self.low_inventory_topic = sns.Topic(
            self, "LowInventoryTopic",
            topic_name="ecom-low-inventory",
            display_name="Ecommerce Low Inventory Alerts",
        )

        self.order_events_topic = sns.Topic(
            self, "OrderEventsTopic",
            topic_name="ecom-order-events",
            display_name="Ecommerce Order Events",
        )

        # ── SQS Queues (subscribers to topics) ───────────────────────────────
        fraud_dlq = sqs.Queue(
            self, "FraudDLQ",
            queue_name="ecom-fraud-dlq",
            retention_period=Duration.days(14),
        )
        self.fraud_queue = sqs.Queue(
            self, "FraudQueue",
            queue_name="ecom-fraud-processing",
            visibility_timeout=Duration.seconds(300),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=fraud_dlq),
        )
        self.fraud_topic.add_subscription(subs.SqsSubscription(self.fraud_queue))

        order_dlq = sqs.Queue(
            self, "OrderDLQ",
            queue_name="ecom-order-dlq",
            retention_period=Duration.days(14),
        )
        self.order_queue = sqs.Queue(
            self, "OrderQueue",
            queue_name="ecom-order-processing",
            visibility_timeout=Duration.seconds(300),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=order_dlq),
        )
        self.order_events_topic.add_subscription(subs.SqsSubscription(self.order_queue))
