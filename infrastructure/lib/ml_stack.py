import os
import shutil
import subprocess
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
from .processing_stack import LocalBundler


class MLStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, storage_stack: StorageStack, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        sagemaker_role = iam.Role(
            self, "SageMakerRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3ReadOnlyAccess"),
            ],
        )
        storage_stack.model_bucket.grant_read_write(sagemaker_role)
        storage_stack.processed_bucket.grant_read(sagemaker_role)

        # SageMaker built-in container account IDs vary by region
        sagemaker_accounts = {
            "us-east-1":      "683313688378",
            "us-east-2":      "257758044811",
            "us-west-1":      "746614075791",
            "us-west-2":      "246618743249",
            "eu-west-1":      "141502667606",
            "eu-west-2":      "764974769150",
            "eu-central-1":   "492215442770",
            "eu-north-1":     "662702820516",
            "ap-southeast-1": "627335202067",
            "ap-northeast-1": "354813040037",
        }
        sm_account = sagemaker_accounts.get(self.region, "683313688378")
        sklearn_image = f"{sm_account}.dkr.ecr.{self.region}.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3"

        # ── SageMaker Model (deployed from model_bucket) ──────────────────────
        self.recommendation_model = sagemaker.CfnModel(
            self, "RecommendationModel",
            model_name="ecom-recommendation-model",
            execution_role_arn=sagemaker_role.role_arn,
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                image=sklearn_image,
                model_data_url=f"s3://{storage_stack.model_bucket.bucket_name}/recommendation/model.tar.gz",
                environment={
                    "SAGEMAKER_PROGRAM": "inference.py",
                    "SAGEMAKER_SUBMIT_DIRECTORY": f"s3://{storage_stack.model_bucket.bucket_name}/recommendation/sourcedir.tar.gz",
                },
            ),
        )

        # NOTE: SageMaker real-time endpoints require a quota increase on new accounts
        # (default limit = 0). The model artifact is deployed to S3. The Lambda below
        # serves recommendations directly from DynamoDB as a fallback.

        # ── Lambda: Recommendation API (catalog-based fallback) ──────────────
        lambda_role = iam.Role(
            self, "RecommendationLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
            ],
        )
        storage_stack.products_table.grant_read_data(lambda_role)

        reco_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../lambdas/recommendation_api"))
        self.recommendation_fn = _lambda.Function(
            self, "RecommendationApi",
            function_name="ecom-recommendation-api",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(
                reco_path,
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    local=LocalBundler(reco_path),
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
                "SAGEMAKER_ENDPOINT": "",  # empty = use catalog fallback
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
