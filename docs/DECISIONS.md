# Decisões de arquitetura

Formato curto: contexto, decisão, consequência.

## 1. Hexagonal com portas pequenas

**Contexto.** O mesmo núcleo precisa rodar sobre Redis, SQS e Service Bus, com execução local ou Celery.
**Decisão.** `ports/` define `Publisher`, `Consumer`, `JobRepository` e `ExecutionBackend`. Serviços só
importam portas; `bootstrap.py` é a única raiz de composição.
**Consequência.** Trocar fornecedor é configuração. O custo é uma camada de tradução por adaptador,
e os recursos exclusivos de cada nuvem (sessões do Service Bus, FIFO do SQS) ficam fora da porta.

## 2. Publisher e Consumer separados (ISP)

Quem só publica (CLI, APIs) não deveria depender de `receive`/`ack`. A interface completa
`MessageQueue` existe só para quem precisa dos dois lados. Isso também permite conceder IAM/RBAC de
produtor e de worker separadamente nos módulos Terraform.

## 3. Entrega pelo menos uma vez, com handlers idempotentes

**Contexto.** SQS, Service Bus e filas Redis confiáveis não garantem exatamente uma entrega.
**Decisão.** O worker confirma (`ack`) só depois de gravar o estado final, ignora jobs que já estão em
estado terminal e o `Dispatcher` aceita `dedup_key` para publicação idempotente.
**Consequência.** Efeitos colaterais dos handlers precisam ser idempotentes. É a regra mais importante
para quem escreve handlers, e por isso aparece no contrato de `JobHandler`.

## 4. Erros classificados na origem (`retryable`)

Em vez de decidir retry por tipo de exceção espalhado pelo código, cada exceção declara `retryable`.
`TransientError` repete com backoff e jitter; `PermanentError` e `JobValidationError` vão direto
para a DLQ depois do registro. As exceções são construídas só com mensagem para sobreviver ao
`pickle` na fronteira entre processos (um erro de pickling aqui derruba o pool inteiro).

## 5. Threads para I/O, processos para CPU

Threads ocupam bem esperas de rede; trabalho de CPU em Python esbarra no GIL, então vai para
`ProcessPoolExecutor` com `spawn` (seguro com threads e sem herdar estado do pai). Se um filho morre,
o pool fica quebrado: o `PoolManager` o recria e os jobs afetados voltam como erro transitório.
O custo do `spawn` é a importação no filho, daí o `TASKFORGE_PLUGINS`.

## 6. Celery como backend de execução, não como fila principal

O Celery pode ser o motor de execução (filas `taskforge.io` e `taskforge.cpu`, `acks_late`, só JSON,
um prefetch baixo), mas a fila de intake continua sendo a porta `MessageQueue`. Assim a escolha de
nuvem não depende do Celery, e quem não precisa dele não paga a complexidade.

## 7. `visibility_timeout` maior que o timeout do job

Se a fila devolver a mensagem antes de o job terminar, ela é executada em paralelo por outro worker.
O Terraform expõe o valor como variável com o aviso, e o `.env.example` repete a regra. Não há
extensão automática de visibilidade ("heartbeat" de lease) nesta versão.

## 8. Estado do job imutável

`Job` é um modelo congelado; cada transição devolve uma cópia validada pela máquina de estados.
Isso elimina uma classe de bugs entre threads e deixa o histórico fácil de testar.

## 9. SQLite como repositório padrão

Suficiente para um nó e para testes, sem infraestrutura. A porta `JobRepository` permite trocar por
Postgres ou DynamoDB; não fiz esse adaptador porque não conseguiria testá-lo sem o serviço real.

## 10. Segurança por padrão

`http.fetch` só aceita destinos que resolvem para IPs públicos e não segue redirecionamentos (SSRF); Celery só usa
JSON (nada de pickle); o container roda sem root, com sistema de arquivos somente leitura e sem
capabilities; o Terraform criptografa filas, exige TLS e separa permissões de produtor e worker.

## 11. Terraform: módulo + ambiente

Módulos não têm provider nem backend, ambientes sim. Isso permite `terraform validate` no CI com
`init -backend=false`. O SNS de alertas usa uma CMK própria (com rotação), porque o CloudWatch não
publica em tópicos cifrados com a chave gerenciada `alias/aws/sns`; a política da chave concede
`kms:Decrypt` e `kms:GenerateDataKey*` ao `cloudwatch.amazonaws.com`, restrito à conta. O Trivy
(AWS-0095) pegou a versão anterior, sem criptografia, na primeira execução do CI.
