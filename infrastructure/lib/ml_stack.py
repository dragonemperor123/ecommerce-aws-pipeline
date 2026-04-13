from aws_cdk import (
    Stack,
    Duration,
    BundlingOptions,
    aws_sagemaker as sagemaker,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_apigateway as apigw,
)
from constructs import Construct
from .storage_stack import StorageStack


class MLStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, storage_stack: StorageStack, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        sagemaker_role = iam.Role(
            self, "SageMakerRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
            ],
        )
        storage_stack.model_bucket.grant_read_write(sagemaker_role)
        storage_stack.processed_bucket.grant_read(sagemaker_role)

        # ── SageMaker Model (deployed from model_bucket) ──────────────────────
        self.recommendation_model = sagemaker.CfnModel(
            self, "RecommendationModel",
            model_name="ecom-recommendation-model",
            execution_role_arn=sagemaker_role.role_arn,
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                image=f"683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3",
                model_data_url=f"s3://{storage_stack.model_bucket.bucket_name}/recommendation/model.tar.gz",
                environment={
                    "SAGEMAKER_PROGRAM": "inference.py",
                    "SAGEMAKER_SUBMIT_DIRECTORY": f"s3://{storage_stack.model_bucket.bucket_name}/recommendation/sourcedir.tar.gz",
                },
            ),
        )

        self.endpoint_config = sagemaker.CfnEndpointConfig(
            self, "RecommendationEndpointConfig",
            endpoint_config_name="ecom-recommendation-config",
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    model_name=self.recommendation_model.model_name,
                    variant_name="AllTraffic",
                    initial_instance_count=1,
                    instance_type="ml.t2.medium",
                    initial_variant_weight=1.0,
                )
            ],
        )
        self.endpoint_config.add_dependency(self.recommendation_model)

        self.endpoint = sagemaker.CfnEndpoint(
            self, "RecommendationEndpoint",
            endpoint_name="ecom-recommendations",
            endpoint_config_name=self.endpoint_config.endpoint_config_name,
        )
        self.endpoint.add_dependency(self.endpoint_config)

        # ── Lambda: Recommendation API (wraps SageMaker endpoint) ────────────
        lambda_role = iam.Role(
            self, "RecommendationLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
            ],
        )
        storage_stack.products_table.grant_read_data(lambda_role)

        self.recommendation_fn = _lambda.Function(
            self, "RecommendationApi",
            function_name="ecom-recommendation-api",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(
                "../lambdas/recommendation_api",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -r . /asset-output",
                    ],
                ),
            ),
            role=lambda_role,
            timeout=Duration.seconds(10),
            memory_size=256,
            environment={
                "SAGEMAKER_ENDPOINT": "ecom-recommendations",
                "PRODUCTS_TABLE": storage_stack.products_table.table_name,
            },
        )

        # ── REST API Gateway ──────────────────────────────────────────────────
        self.api = apigw.RestApi(
            self, "RecommendationRestApi",
            rest_api_name="ecom-recommendations-api",
            description="Real-time product recommendations",
            deploy_options=apigw.StageOptions(
                stage_name="prod",
                throttling_rate_limit=1000,
                throttling_burst_limit=500,
                metrics_enabled=True,
                logging_level=apigw.MethodLoggingLevel.INFO,
            ),
        )

        recommendations_resource = self.api.root.add_resource("recommendations")
        recommendations_resource.add_method(
            "POST",
            apigw.LambdaIntegration(self.recommendation_fn, proxy=True),
        )
