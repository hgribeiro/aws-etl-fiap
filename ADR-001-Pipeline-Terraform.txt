
# ADR-001: Arquitetura do Pipeline de Dados com Terraform

**Status:** Proposto

## Contexto

O objetivo é construir um pipeline de dados na AWS para atender aos requisitos do projeto. Este pipeline será responsável por extrair dados diários de ações, processá-los e disponibilizá-los para análise via SQL. A infraestrutura para este pipeline deve ser criada e gerenciada como código (IaC) utilizando Terraform.

## Requisitos do Pipeline

*   **Requisito 1:** Scrap de dados de ações ou índices da B3 (granularidade diária).
*   **Requisito 2:** Os dados brutos devem ser ingeridos no S3 em formato parquet com partição diária.
*   **Requisito 3:** O bucket deve acionar uma lambda, que por sua vez irá chamar o job de ETL no Glue.
*   **Requisito 4:** A lambda pode ser em qualquer linguagem. Ela apenas deverá iniciar o job Glue.
*   **Requisito 5:** O job Glue deve conter as seguintes transformações obrigatórias:
    *   A: Agrupamento numérico, sumarização, contagem ou soma.
    *   B: Renomear duas colunas existentes.
    *   C: Realizar um cálculo com base na data.
*   **Requisito 6:** Os dados refinados no job Glue devem ser salvos no formato parquet em uma pasta chamada `refined`, particionado por data e pelo nome ou código da ação/índice.
*   **Requisito 7:** O job Glue deve automaticamente catalogar o dado no Glue Catalog e criar uma tabela.
*   **Requisito 8:** Os dados devem estar disponíveis e serem consultados usando SQL através do Athena.

## Fluxo da Arquitetura Proposta

1.  **Agendamento (Diário):** O Amazon EventBridge Scheduler aciona o primeiro job do AWS Glue, responsável pela extração (**Requisito 1**).
2.  **Extração (Glue Job 1):** O job de extração busca os dados de ações (ex: `yfinance`) e os salva em formato Parquet no prefixo `raw/` de um bucket S3, com partição diária (**Requisito 2**).
3.  **Gatilho (S3/Lambda):** A criação de um novo objeto no bucket S3 aciona uma função Lambda (`glue_starter_lambda`) (**Requisito 3**).
4.  **Início do ETL (Lambda):** A função Lambda `glue_starter_lambda` inicia o segundo job do AWS Glue, responsável pela transformação (**Requisito 4**).
5.  **Transformação (Glue Job 2):** O job de transformação processa os dados do prefixo `raw/`, aplica as transformações (**Requisito 5**) e salva o resultado no prefixo `refined/` com as partições corretas (**Requisito 6**).
6.  **Catálogo e Consulta (Glue/Athena):** O job de transformação atualiza o catálogo de dados (**Requisito 7**), tornando os dados refinados consultáveis via Amazon Athena (**Requisito 8**).

## Decisão

Adotaremos o Terraform para provisionar toda a infraestrutura na AWS. A estrutura será modular, alinhada com os componentes do pipeline:

1.  **Gerenciamento de Estado do Terraform (Backend S3):**
    *   Para garantir a segurança e a colaboração, o arquivo de estado do Terraform (`.tfstate`) será armazenado remotamente em um bucket S3 dedicado.
    *   Isso resolve a desvantagem do gerenciamento de estado local.

2.  **EventBridge:**
    *   Um `aws_scheduler_schedule` será criado para acionar o job do Glue de extração diariamente, cumprindo o **Requisito 1**.

3.  **S3 (Simple Storage Service):**
    *   Um único bucket S3 (`aws_s3_bucket`) será criado para armazenar os dados e os scripts da aplicação.
    *   O bucket terá os prefixos: `raw/` (**Requisito 2**), `refined/` (**Requisito 6**) e `scripts/`.

4.  **Função Lambda:**
    *   **Lambda de Início do Glue:** Uma função (`aws_lambda_function`) com o código de `src/glue_starter_lambda_function.py`, acionada por eventos S3 para iniciar o job de transformação do Glue, atendendo aos **Requisitos 3 e 4**.
    *   A função terá uma IAM Role (`aws_iam_role`) com permissões específicas para iniciar um job do Glue.

5.  **Glue (ETL e Catálogo de Dados):**
    *   **Glue Job de Extração:** Um `aws_glue_job` para extrair os dados. O script (`src/glue_extractor_job.py`) conterá a lógica para os **Requisitos 1 e 2**.
    *   **Glue Job de Transformação:** Um segundo `aws_glue_job` para transformar os dados. O script (`src/glue_transformer_job.py`) conterá a lógica para o **Requisito 5**.
    *   **Glue Catalog:** Um banco de dados (`aws_glue_catalog_database`) será criado. O job de transformação será responsável por catalogar os dados refinados, conforme o **Requisito 7**.
    *   Os jobs terão IAM Roles com permissões apropriadas.

6.  **Scripts da Aplicação:**
    *   `src/glue_extractor_job.py`: Script PySpark para o job de extração do Glue.
    *   `src/glue_transformer_job.py`: Script PySpark para o job de transformação do Glue.
    *   `src/glue_starter_lambda_function.py`: Script Python para a função Lambda que aciona o job de transformação.

### Estrutura de Módulos Terraform Proposta

```
infra/
├── main.tf             # Orquestrador dos módulos
├── backend.tf          # Configuração do backend S3
├── ...
├── tf-backend/         # (Separado) Módulo para criar o bucket do .tfstate
│   └── main.tf
├── eventbridge/
│   └── main.tf         # Regra do EventBridge Scheduler para acionar o Glue
├── lambda-glue-starter/
│   └── main.tf         # Lambda de início do Glue + IAM Role + Gatilho S3
├── glue/
│   └── main.tf         # Glue Jobs (extração e transformação), Glue Database, IAM Roles
└── s3/
    └── main.tf         # S3 Bucket para dados e scripts
```

## Consequências

### Vantagens
*   **Reprodutibilidade:** A infraestrutura pode ser recriada de forma consistente.
*   **Versionamento:** Toda a arquitetura é versionada no Git.
*   **Modularidade:** A separação de recursos facilita a manutenção.
*   **Automação:** O pipeline é totalmente agendado e automatizado.
*   **Estado Seguro:** O estado do Terraform é gerenciado de forma segura e remota.
*   **Separação de Responsabilidades:** Jobs de extração e transformação são desacoplados, permitindo que sejam executados e mantidos de forma independente.

### Desvantagens
*   **Complexidade Inicial:** A configuração do Terraform e dos módulos requer conhecimento da ferramenta.
*   **Custos:** Múltiplos jobs e uma função Lambda incorrerão em custos.
*   **Maior Complexidade da Arquitetura:** O fluxo com dois jobs e uma função Lambda é mais complexo de orquestrar e depurar do que uma solução com um único job.
*   **Gerenciamento de Dependências no Glue:** As bibliotecas Python (como `yfinance`) precisam ser corretamente configuradas no ambiente do job de extração.
