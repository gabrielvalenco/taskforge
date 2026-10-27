locals {
  dlq_name = "${var.name}-dlq"
  use_cmk  = var.kms_key_arn != null
}

# --- Dead-letter queue ----------------------------------------------------------------------

resource "aws_sqs_queue" "dlq" {
  name                      = local.dlq_name
  message_retention_seconds = var.dlq_retention_seconds

  # SSE-SQS e CMK são mutuamente exclusivos.
  sqs_managed_sse_enabled = local.use_cmk ? null : true
  kms_master_key_id       = var.kms_key_arn

  tags = var.tags
}

# --- Fila principal -------------------------------------------------------------------------

resource "aws_sqs_queue" "main" {
  name                       = var.name
  visibility_timeout_seconds = var.visibility_timeout_seconds
  message_retention_seconds  = var.message_retention_seconds
  receive_wait_time_seconds  = 20 # long polling: menos requests vazios, menor custo e latência

  sqs_managed_sse_enabled = local.use_cmk ? null : true
  kms_master_key_id       = var.kms_key_arn

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })

  tags = var.tags
}

# Só a fila principal pode usar esta DLQ como destino (evita que outra fila "roube" a DLQ).
resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.main.arn]
  })
}

# Nega qualquer acesso sem TLS (boa prática exigida por CIS/Security Hub).
resource "aws_sqs_queue_policy" "tls_only" {
  for_each = {
    main = aws_sqs_queue.main
    dlq  = aws_sqs_queue.dlq
  }

  queue_url = each.value.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "sqs:*"
      Resource  = each.value.arn
      Condition = {
        Bool = { "aws:SecureTransport" = "false" }
      }
    }]
  })
}

# --- IAM de menor privilégio: um policy para quem publica, outro para quem consome ----------

data "aws_iam_policy_document" "producer" {
  statement {
    sid       = "PublishJobs"
    actions   = ["sqs:SendMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.main.arn]
  }

  dynamic "statement" {
    for_each = local.use_cmk ? [1] : []
    content {
      sid       = "EncryptWithCmk"
      actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
      resources = [var.kms_key_arn]
    }
  }
}

data "aws_iam_policy_document" "worker" {
  statement {
    sid = "ConsumeJobs"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueUrl",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.main.arn]
  }

  # Dead-letter explícito (nack sem requeue) publica na DLQ e apaga da fila principal.
  statement {
    sid       = "DeadLetterExplicitly"
    actions   = ["sqs:SendMessage", "sqs:GetQueueUrl"]
    resources = [aws_sqs_queue.dlq.arn]
  }

  dynamic "statement" {
    for_each = local.use_cmk ? [1] : []
    content {
      sid       = "DecryptWithCmk"
      actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
      resources = [var.kms_key_arn]
    }
  }
}

resource "aws_iam_policy" "producer" {
  name        = "${var.name}-producer"
  description = "Permite publicar jobs em ${var.name}."
  policy      = data.aws_iam_policy_document.producer.json
  tags        = var.tags
}

resource "aws_iam_policy" "worker" {
  name        = "${var.name}-worker"
  description = "Permite consumir jobs de ${var.name} e escrever na DLQ."
  policy      = data.aws_iam_policy_document.worker.json
  tags        = var.tags
}

# --- Alarmes --------------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.name}-dlq-not-empty"
  alarm_description   = "Há jobs na dead-letter queue de ${var.name}: investigue e reprocesse."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.dlq_alarm_threshold
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }

  alarm_actions = var.alarm_topic_arns
  ok_actions    = var.alarm_topic_arns
  tags          = var.tags
}

resource "aws_cloudwatch_metric_alarm" "oldest_message_age" {
  alarm_name          = "${var.name}-oldest-message-age"
  alarm_description   = "Mensagens esperando há muito tempo em ${var.name}: workers parados ou lentos."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.oldest_message_age_threshold_seconds
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.main.name
  }

  alarm_actions = var.alarm_topic_arns
  ok_actions    = var.alarm_topic_arns
  tags          = var.tags
}
