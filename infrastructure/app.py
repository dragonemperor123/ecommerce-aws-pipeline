#!/usr/bin/env python3
import aws_cdk as cdk
from lib.ingestion_stack import IngestionStack
from lib.storage_stack import StorageStack
from lib.processing_stack import ProcessingStack
from lib.ml_stack import MLStack
from lib.notification_stack import NotificationStack
from lib.monitoring_stack import MonitoringStack

app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-1",
)

storage = StorageStack(app, "EcomStorageStack", env=env)
ingestion = IngestionStack(app, "EcomIngestionStack", storage_stack=storage, env=env)
notification = NotificationStack(app, "EcomNotificationStack", env=env)
processing = ProcessingStack(
    app, "EcomProcessingStack",
    storage_stack=storage,
    ingestion_stack=ingestion,
    notification_stack=notification,
    env=env,
)
ml = MLStack(app, "EcomMLStack", storage_stack=storage, env=env)
monitoring = MonitoringStack(
    app, "EcomMonitoringStack",
    ingestion_stack=ingestion,
    processing_stack=processing,
    env=env,
)

app.synth()
