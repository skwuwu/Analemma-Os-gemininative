from src.aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_kms as kms,
    CfnOutput,
)
from src.constructs import Construct


class ServerlessWorkflowDatabaseStack(Stack):
    """
    [Improved] Single Responsibility Principle: Define only DynamoDB tables and data layer
    
    Previous name: ServerlessWorkflowStack
    New name: ServerlessWorkflowDatabaseStack
    
    Responsibilities:
    - Define DynamoDB tables (Workflows, TaskTokens, Users)
    - Set up GSI and indexes
    - Table encryption and TTL settings
    - ARN output for cross-stack reference
    
    Excluded responsibilities (moved to separate stack):
    - Lambda functions (to ComputeStack/ApiStack)
    - EventBridge rules (to ComputeStack)
    - Step Functions (to ComputeStack)
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        workflows_table_name: str = "Workflows",
        task_tokens_table_name: str = "TaskTokens",
        users_table_name: str = "Users",
        enable_tasktokens_cmk: bool = False,
        # Optional list of IAM role ARNs (e.g. Lambda roles) to grant KMS usage
        tasktokens_cmk_grant_role_arns: list[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # =====================================================================
        # 1. Workflows Table: Core table for storing workflow designs
        # =====================================================================
        # Stores JSON blueprints and metadata of user-created workflows.
        # NOTE: `is_scheduled` is stored as STRING ("true"/"false") because
        # DynamoDB does not allow BOOLEAN types as key attributes (GSI PK/SK
        # must be STRING, NUMBER or BINARY). Keep this in mind when writing
        # scheduler code that queries the ScheduledWorkflowsIndex.

        workflows_table = dynamodb.Table(
            self,
            "WorkflowsTable",
            table_name=workflows_table_name,
            partition_key=dynamodb.Attribute(
                name="ownerId",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="workflowId",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # --- Add Global Secondary Index (GSI) ---
        # [Improved] Sharding-based scheduler GSI: Prevent "hot partitions" for large-scale scalability
        # Used by central scheduler lambda to efficiently find workflows that "should run now".
        # 
        # Sharding strategy:
        # - PK: schedule_shard_id ("shard_0" ~ "shard_9", randomly assigned when saving workflow)
        # - SK: next_run_time (execution time)
        # - Scheduler queries 10 shards in parallel to distribute read load.
        workflows_table.add_global_secondary_index(
            index_name="ScheduledWorkflowsIndexV2",
            partition_key=dynamodb.Attribute(
                name="schedule_shard_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="next_run_time",
                type=dynamodb.AttributeType.NUMBER
            )
        )

        # --- OwnerId/Name GSI: support efficient lookup by (ownerId, name)
        # This index is required by the get_workflow_by_name Lambda which
        # queries OwnerIdNameIndex with ownerId as partition key and name as sort key.
        workflows_table.add_global_secondary_index(
            index_name="OwnerIdNameIndexV2",
            partition_key=dynamodb.Attribute(
                name="ownerId",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="name",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # -----------------------------------------------------------------
        # [Removed] MergeCallback Lambda moved to ComputeStack/ApiStack
        # Lambda functions should be defined in separate stacks and
        # reference these tables via cross-stack references.
        # -----------------------------------------------------------------

        # =====================================================================
        # 2. Task Tokens Table: Table for storing Human-in-the-Loop(HITP) state
        # =====================================================================
        # Temporarily stores TaskTokens created when Step Functions are paused.
        # Optionally use a customer-managed KMS key for stronger data-at-rest
        # protection for TaskTokens, which contain sensitive taskToken values.
        task_tokens_encryption = dynamodb.TableEncryption.AWS_MANAGED
        task_tokens_key = None
        if enable_tasktokens_cmk:
            task_tokens_key = kms.Key(self, "TaskTokensCMK", enable_key_rotation=True)
            # Ensure DynamoDB service can use the CMK for table encryption ops
            try:
                from src.aws_cdk import aws_iam as iam

                # Allow DynamoDB service principal to use the key
                task_tokens_key.add_to_resource_policy(iam.PolicyStatement(
                    actions=[
                        "kms:Encrypt",
                        "kms:Decrypt",
                        "kms:ReEncrypt*",
                        "kms:GenerateDataKey*",
                        "kms:DescribeKey",
                    ],
                    principals=[iam.ServicePrincipal("dynamodb.amazonaws.com")],
                    resources=["*"],
                ))

                # Optionally grant the provided role ARNs (Lambda roles) permission
                if tasktokens_cmk_grant_role_arns:
                    for arn in tasktokens_cmk_grant_role_arns:
                        task_tokens_key.add_to_resource_policy(iam.PolicyStatement(
                            actions=[
                                "kms:Decrypt",
                                "kms:Encrypt",
                                "kms:ReEncrypt*",
                                "kms:GenerateDataKey*",
                                "kms:DescribeKey",
                            ],
                            principals=[iam.ArnPrincipal(arn)],
                            resources=["*"],
                        ))
            except Exception:
                # If aws_iam isn't available at synth time in this environment,
                # continue; real CDK synth will have the module.
                pass
            task_tokens_encryption = dynamodb.TableEncryption.CUSTOMER_MANAGED

        # TaskTokens table uses a composite key to scope tokens to a tenant (ownerId).
        # Partition key: ownerId (tenant/owner)
        # Sort key: conversation_id (unique per conversation within owner)
        task_tokens_table = dynamodb.Table(
            self,
            "TaskTokensTable",
            table_name=task_tokens_table_name,
            partition_key=dynamodb.Attribute(
                name="ownerId",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="conversation_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
            encryption=task_tokens_encryption,
            encryption_key=task_tokens_key,
        )

        # Add GSI to support efficient lookup by execution_id within a tenant.
        # GSI partition: ownerId, sort: execution_id
        task_tokens_table.add_global_secondary_index(
            index_name="ExecutionIdIndexV2",
            partition_key=dynamodb.Attribute(
                name="ownerId",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="execution_id",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

        # =====================================================================
        # 3. Users Table: Table for storing user information
        # =====================================================================
        # Stores basic user information. GSI etc. can be added as needed.
        users_table = dynamodb.Table(
            self,
            "UsersTable",
            table_name=users_table_name,
            partition_key=dynamodb.Attribute(
                name="userId",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # --- CloudFormation Outputs: Support for cross-stack references ---
        # Output both table names and ARNs so that other stacks
        # (ComputeStack, ApiStack) can reference these tables.
        
        # Table Names (maintain existing compatibility)
        CfnOutput(self, "WorkflowsTableName", value=workflows_table.table_name)
        CfnOutput(self, "TaskTokensTableName", value=task_tokens_table.table_name) 
        CfnOutput(self, "UsersTableName", value=users_table.table_name)
        
        # Table ARNs (for cross-stack references)
        CfnOutput(self, "WorkflowsTableArn", value=workflows_table.table_arn,
                 export_name="DatabaseStack-WorkflowsTableArn")
        CfnOutput(self, "TaskTokensTableArn", value=task_tokens_table.table_arn,
                 export_name="DatabaseStack-TaskTokensTableArn") 
        CfnOutput(self, "UsersTableArn", value=users_table.table_arn,
                 export_name="DatabaseStack-UsersTableArn")
        
        # GSI Names (used by scheduler etc.)
        CfnOutput(self, "ScheduledWorkflowsIndexName", value="ScheduledWorkflowsIndexV2",
                 export_name="DatabaseStack-ScheduledWorkflowsIndexName")
        CfnOutput(self, "OwnerIdNameIndexName", value="OwnerIdNameIndexV2",
                 export_name="DatabaseStack-OwnerIdNameIndexName")
        CfnOutput(self, "ExecutionIdIndexName", value="ExecutionIdIndexV2",
                 export_name="DatabaseStack-ExecutionIdIndexName")

        # -----------------------------------------------------------------
        # [Improved] Lambda and EventBridge defined in separate stacks
        # -----------------------------------------------------------------
        # The SchedulerFunction previously defined here is now defined in ComputeStack
        # and uses the table ARNs below via cross-stack references:
        # 
        # Example in ComputeStack:
        # from src.aws_cdk import Fn
        # workflows_table_arn = Fn.import_value("DatabaseStack-WorkflowsTableArn")
        # workflows_table = dynamodb.Table.from_table_arn(self, "ImportedWorkflowsTable", workflows_table_arn)
        # 
        # SchedulerFunction sharding query example:
        # for shard_id in range(10):  # shard_0 ~ shard_9
        #     response = table.query(
        #         IndexName="ScheduledWorkflowsIndex",
        #         KeyConditionExpression=Key('schedule_shard_id').eq(f'shard_{shard_id}') & 
        #                               Key('next_run_time').lt(current_time)
        #     )
        # -----------------------------------------------------------------
