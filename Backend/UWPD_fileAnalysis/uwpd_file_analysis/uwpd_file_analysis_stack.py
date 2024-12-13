# from aws_cdk import (
#     # Duration,
#     Stack,
#     # aws_sqs as sqs,
# )
# from constructs import Construct

# class UwpdFileAnalysisStack(Stack):

#     def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
#         super().__init__(scope, construct_id, **kwargs)

#         # The code that defines your stack goes here

#         # example resource
#         # queue = sqs.Queue(
#         #     self, "UwpdFileAnalysisQueue",
#         #     visibility_timeout=Duration.seconds(300),
#         # )
# uwpd_analysis_stack.py

from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    CfnOutput,
    Duration,
    RemovalPolicy
)
from constructs import Construct
import os


class UwpdFileAnalysisStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, *, project_name: str, bucket_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 Bucket with specified name
        analysis_bucket = s3.Bucket(
            self, "AnalysisBucket",
            bucket_name=bucket_name,
            removal_policy=RemovalPolicy.DESTROY,  # Change as needed
            auto_delete_objects=True,  # Change as needed
        )

        # Add folders by creating zero-byte objects
        s3_deployment.BucketDeployment(
            self, "CreateDispatchedFolder",
            destination_bucket=analysis_bucket,
            destination_key_prefix="dispatched/",
            sources=[s3_deployment.Source.data("placeholder.txt", "This is a placeholder file.")],
            retain_on_delete=False
        )
        s3_deployment.BucketDeployment(
            self, "CreateArrivalFolder",
            destination_bucket=analysis_bucket,
            destination_key_prefix="arrival/",
            sources=[s3_deployment.Source.data("placeholder.txt", "This is a placeholder file.")],
            retain_on_delete=False
        )
        s3_deployment.BucketDeployment(
            self, "CreateAnalysisFolder",
            destination_bucket=analysis_bucket,
            destination_key_prefix="analysis/",
            sources=[s3_deployment.Source.data("placeholder.txt", "This is a placeholder file.")],
            retain_on_delete=False
        )
        

        # IAM Role for Lambda
        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        # Custom Policy for S3 Access
        s3_access_policy = iam.Policy(
            self, "S3AccessPolicy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:ListBucket",
                        "s3:GetObject",
                        "s3:PutObject"
                    ],
                    resources=[
                        analysis_bucket.bucket_arn,
                        analysis_bucket.arn_for_objects("*")
                    ]
                )
            ]
        )
        s3_access_policy.attach_to_role(lambda_role)

        #Defining the Default Pandas Layer

        pandas_layer = _lambda.LayerVersion.from_layer_version_arn(
            self, "AWSSDKPandasLayer",
            layer_version_arn="arn:aws:lambda:us-west-2:336392948345:layer:AWSSDKPandas-Python312:13"
        )

        # Lambda Function
        lambda_function = _lambda.Function(
            self, "AnalysisLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/analysis_handler"),
            role=lambda_role,
            environment={
                "BUCKET_NAME": bucket_name
            },
            function_name=f"{project_name}-AnalysisFunction",
            timeout=Duration.minutes(5),
            layers=[pandas_layer]
        )

        # Create Lambda Function URL with CORS enabled
        function_url = lambda_function.add_function_url(
            auth_type=_lambda.FunctionUrlAuthType.NONE,
            cors={
                "allowed_origins": ["*"], 
                "allowed_methods": [
                    _lambda.HttpMethod.GET,
                    _lambda.HttpMethod.POST
                ],
            }
        )

        # Output the Function URL
        CfnOutput(
            self, "LambdaFunctionURL",
            value=function_url.url,
            description="URL of the Lambda Function",
        )
