# AWS ETL Fiap

Este projeto implementa um pipeline de ETL (Extração, Transformação e Carga) para dados do mercado financeiro, utilizando serviços da AWS. O objetivo é extrair dados de ações, processá-los e disponibilizá-los para consulta via SQL com o Amazon Athena.

Toda a infraestrutura é provisionada como código utilizando Terraform, conforme definido no documento de decisão de arquitetura [ADR-001](ADR-001-Pipeline-Terraform.md).

## Arquitetura do Pipeline

A arquitetura desacoplada utiliza múltiplos serviços da AWS para criar um pipeline robusto e escalável:

1.  **Agendamento (EventBridge):** Um agendamento diário no Amazon EventBridge aciona o primeiro job do AWS Glue, iniciando o pipeline.
2.  **Extração (AWS Glue):** O primeiro job (`glue_extractor_job.py`) é responsável por extrair dados de ações da internet (usando a biblioteca `yfinance`) e salvá-los em formato Parquet no bucket S3, dentro do prefixo `raw/`.
3.  **Gatilho (S3 + Lambda):** A chegada de um novo arquivo em `raw/` aciona uma função AWS Lambda (`glue_starter_lambda_function.py`).
4.  **Orquestração (Lambda):** A função Lambda atua como um orquestrador, iniciando a execução do segundo job do AWS Glue.
5.  **Transformação (AWS Glue):** O segundo job (`glue_transformer_job.py`) lê os dados brutos de `raw/`, aplica as transformações de negócio necessárias e salva os dados enriquecidos no prefixo `refined/`.
6.  **Catálogo de Dados (Glue Catalog):** Ao final do processo, o job de transformação atualiza o AWS Glue Data Catalog, tornando os dados disponíveis para consulta.
7.  **Análise (Amazon Athena):** Os dados finais em `refined/` podem ser consultados diretamente via SQL utilizando o Amazon Athena.

### Estrutura do S3

Os dados são organizados no S3 da seguinte maneira:

- `s3://<bucket-name>/raw/`: Armazena os dados brutos extraídos pela primeira etapa do pipeline, particionados por data.
- `s3://<bucket-name>/refined/`: Armazena os dados processados e enriquecidos, prontos para análise, também particionados.
- `s3://<bucket-name>/scripts/`: Armazena os scripts Python e PySpark utilizados pelos jobs do Glue e pela função Lambda.

## Scripts do Projeto

-   `src/glue_extractor_job.py`: Script PySpark para o job de extração do Glue.
-   `src/glue_transformer_job.py`: Script PySpark para o job de transformação do Glue.
-   `src/glue_starter_lambda_function.py`: Script Python para a função Lambda que aciona o job de transformação.

## Infraestrutura como Código (Terraform)

A infraestrutura é definida em módulos do Terraform, localizados no diretório `infra/`. A estrutura modular inclui:

-   `eventbridge/`: Define o agendamento do pipeline.
-   `lambda-glue-starter/`: Define a função Lambda e seu gatilho S3.
-   `glue/`: Define os dois jobs do Glue, o Data Catalog e as permissões necessárias.
-   `s3/`: Define o bucket S3 para armazenamento dos dados.

Consulte a [ADR-001](ADR-001-Pipeline-Terraform.md) para mais detalhes sobre as decisões de implementação do Terraform.

## Dependências

As dependências Python do projeto estão listadas no arquivo `requirements.txt`:

- `yfinance`: Para extração de dados do Yahoo Finance.