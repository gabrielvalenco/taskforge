# Infraestrutura (Terraform)

```
modules/aws_queue     SQS + DLQ + redrive + IAM + alarmes
modules/azure_queue   Service Bus namespace + fila + RBAC + alerta
envs/aws              raiz que usa o módulo AWS (backend S3)
envs/azure            raiz que usa o módulo Azure (backend azurerm)
```

## Uso

```bash
cd envs/aws                     # ou envs/azure
cp backend.hcl.example backend.hcl && cp terraform.tfvars.example terraform.tfvars
terraform init -backend-config=backend.hcl
terraform plan -out tf.plan
terraform apply tf.plan
terraform output taskforge_env  # AWS: variáveis para os workers
```

Validação sem credenciais nem backend (é o que o CI faz):

```bash
terraform fmt -check -recursive
terraform init -backend=false && terraform validate
```

## Decisões de segurança

- Criptografia em repouso nas filas (SSE-SQS por padrão, CMK opcional via `kms_key_arn`).
- Política da fila SQS nega qualquer acesso sem TLS.
- Políticas IAM separadas para produtor (só envia) e worker (recebe, apaga, ajusta visibilidade).
- Service Bus com TLS mínimo 1.2; `local_auth_enabled = false` desliga SAS e força Entra ID.
- O tópico SNS de alertas usa uma CMK própria com rotação: alarmes do CloudWatch não conseguem
  publicar em tópicos cifrados com a chave gerenciada `alias/aws/sns`, então a política da chave
  concede `kms:Decrypt` e `kms:GenerateDataKey*` ao `cloudwatch.amazonaws.com` (restrito à conta).
- O estado do Terraform contém segredos (connection string do Service Bus). Use backend remoto com
  criptografia e acesso restrito, e nunca versione `*.tfstate`, `*.tfvars` ou `backend.hcl`.

## Regra que mais quebra em produção

`visibility_timeout_seconds` (AWS) e `lock_duration_seconds` (Azure) precisam ser **maiores** que
`TASKFORGE_JOB_TIMEOUT_SECONDS`. Caso contrário a mensagem reaparece enquanto o job ainda executa e
é processada duas vezes.

## Não validado

Estes arquivos não foram aplicados nem validados com o binário do Terraform no ambiente em que foram
escritos. Rode `terraform validate` e `plan` antes de confiar neles.
