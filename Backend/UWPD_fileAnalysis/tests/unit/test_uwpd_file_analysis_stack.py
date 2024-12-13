import aws_cdk as core
import aws_cdk.assertions as assertions

from uwpd_file_analysis.uwpd_file_analysis_stack import UwpdFileAnalysisStack

# example tests. To run these tests, uncomment this file along with the example
# resource in uwpd_file_analysis/uwpd_file_analysis_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = UwpdFileAnalysisStack(app, "uwpd-file-analysis")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
