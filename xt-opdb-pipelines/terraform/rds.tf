resource "aws_secretsmanager_secret" "rds_credentials" {
  name = "read_replica_credentials"
}

# initial screts with no actual value
resource "aws_secretsmanager_secret_version" "rds_credentials" {
  secret_id     = aws_secretsmanager_secret.rds_credentials.id
  secret_string = <<EOF
{
  "db_username": "",
  "db_password": "",
  "engine": "",
  "server_hostname": "",
  "db_port": "",
  "db_domain": "",
  "database": ""
}
EOF
}
