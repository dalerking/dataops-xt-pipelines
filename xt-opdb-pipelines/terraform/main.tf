module "eventbridge_rule" {
  source = "github.com/variant-inc/terraform-aws-eventbridge-rule?ref=v1"
  for_each = toset([
    "xt-broker-driver-info",
    "xt-broker-load-assignment",
    "xt-broker-offers",
    "xt-broker-posted-trucks",
    "xt-ml-recommendations",
    "xt-product-amplitude",
    "xt-rolled-reason",
  ])

  name        = each.key
  description = "EventBridge rule for processing XT pipelines"

  schedule_expression = "rate(5 minutes)"
  is_enabled          = true
  event_targets       = {
    "lambda-target": {
      "arn": "arn:aws:lambda:us-east-1:648462982672:function:data_xt_opdb_pipelines_api",
      "input": {
        "data": {
          "databaseDetails": {
            "secretManager": "xt_database_creds"
          },
          "jobDetails": {
            "pipelineName": each.key,
            "secretManager": "xt-elasticsearch-creds"
          },
        }
      }
    }
  }
}

module "lambda" {
  source = "github.com/variant-inc/terraform-aws-lambda?ref=v1"

  name        = "data_xt_opdb_pipelines_api"
  image_uri   = var.image_uri
  policy      = var.policy
  timeout     = 900
  memory_size = 8192
  vpc_config = { security_group_ids = ["sg-0fc9abd2557d40c6b", "sg-04392fdb0110af319"]
    subnet_ids = ["subnet-04471c2e13a7bdfd0", "subnet-0cfe71028e69dd125",
      "subnet-03b5ad508d4bc0a86",
  "subnet-0798c9c27623faee6"] }
}
