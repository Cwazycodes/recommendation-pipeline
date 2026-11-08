resource "aws_dynamodb_table" "trending" {
  name         = local.table.TableName
  billing_mode = local.table.BillingMode
  hash_key     = one([for key in local.table.KeySchema : key.AttributeName if key.KeyType == "HASH"])
  range_key    = one([for key in local.table.KeySchema : key.AttributeName if key.KeyType == "RANGE"])

  dynamic "attribute" {
    for_each = local.table.AttributeDefinitions

    content {
      name = attribute.value.AttributeName
      type = attribute.value.AttributeType
    }
  }

  ttl {
    attribute_name = local.table.TimeToLiveSpecification.AttributeName
    enabled        = local.table.TimeToLiveSpecification.Enabled
  }
}
