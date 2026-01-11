# AWS ETL Fiap

Este projeto implementa um pipeline de ETL (Extração, Transformação e Carga) para dados do mercado financeiro, utilizando serviços da AWS. O objetivo é extrair dados de ações, processá-los e armazená-los no Amazon S3 de forma estruturada.

## Arquitetura

A arquitetura utiliza os seguintes serviços da AWS:

- **EventBridge**: Utilizado para iniciar a execução agendada da máquina de estado do Step Functions, permitindo execuções periódicas do pipeline (ex: diariamente).
- **AWS Step Functions**: Orquestra o fluxo de trabalho ETL, gerenciando a execução de cada etapa de forma coordenada e resiliente.
- **Amazon S3 (Simple Storage Service)**: Armazena os dados em diferentes estágios do processo ETL.


### Estrutura do S3

Os dados são organizados no S3 da seguinte maneira:

- `s3://fiap-etl/raw/`: Armazena os dados brutos extraídos da fonte (Yahoo Finance).
- `s3://fiap-etl/interim/`: Armazena os dados após a etapa de transformação.
- `s3://fiap-etl/final/`: Armazena os dados finais, prontos para consumo por outras aplicações ou para análise.

## Execução

O coração do ETL é o script `etl.py`, que é executado em um ambiente containerizado com Docker. A imagem Docker é construída a partir do `Dockerfile` no projeto.

O AWS Step Functions é configurado para executar este container como uma tarefa, passando os parâmetros necessários para a execução do script.

### Script `etl.py`

Este script utiliza a biblioteca `yfinance` para extrair dados históricos de ações. Atualmente, está configurado para extrair dados do ticker `ITUB4.SA`.

## Desenvolvimento Local com LocalStack

É possível executar e testar a configuração do Terraform localmente utilizando o [LocalStack](https://localstack.cloud/). A conexão entre o Terraform e o LocalStack é configurada no arquivo `infra/iam/providers.tf`, que aponta o provedor AWS para os endpoints do LocalStack (ex: `http://localhost:4566`).

### Iniciando o LocalStack com Docker Compose

Para facilitar a execução do LocalStack, você pode usar o arquivo `docker-compose.yml` na raiz do projeto.

1.  Inicie o LocalStack em modo detached:
    ```bash
    docker-compose up -d
    ```

2.  Para parar o LocalStack:
    ```bash
    docker-compose down
    ```

### Pré-requisitos

-   [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli) instalado.
-   [LocalStack](https://docs.localstack.cloud/getting-started/installation/) instalado e em execução (via Docker Compose ou outro método).
-   [AWS CLI](https://aws.amazon.com/cli/) instalado.

### Executando o Terraform

1.  Navegue até o diretório `infra/iam`:
    ```bash
    cd infra/iam
    ```

2.  Inicialize o Terraform:
    ```bash
    terraform init
    ```

3.  Aplique a configuração do Terraform:
    ```bash
    terraform apply
    ```

Isso irá criar os recursos da AWS (no ambiente LocalStack) definidos nos arquivos `.tf`.

### Verificando os recursos

Após aplicar a configuração do Terraform, você pode verificar se os recursos foram criados corretamente no LocalStack usando o AWS CLI.

Por exemplo, para verificar se o "role" do IAM foi criado:
```bash
aws --endpoint-url=http://localhost:4566 iam get-role --role-name fiap-etl-role
```

Se o comando retornar um JSON com os detalhes do "role", a conexão e a criação dos recursos foram bem-sucedidas.

## Dependências

As dependências Python do projeto estão listadas no arquivo `requirements.txt`:

- `yfinance`: Para extração de dados do Yahoo Finance.
 