provider "aws" {
    region = "us-east-1"
    default_tags {
        tags = {
        team : "DataOps",
        purpose : "XT Pipeline EventBridge Rule",
        owner : "Dale"
        }
    }
  }
