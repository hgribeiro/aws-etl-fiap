"""
Script para coletar dados de ações da B3 usando yfinance no AWS Glue
"""
import sys
import logging
from datetime import datetime, timedelta
from typing import List

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp, col, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, TimestampType, DateType
)

import yfinance as yf
import pandas as pd
import boto3
from botocore.exceptions import ClientError

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_stock_data(ticker: str, period: str = "1y") -> pd.DataFrame:
    """Coleta dados históricos de uma ação"""
    ticker_sa = f"{ticker}.SA" if not ticker.endswith('.SA') else ticker
    stock = yf.Ticker(ticker_sa)
    df = stock.history(period=period)
    
    if not df.empty:
        # IMPORTANTE: Garantir que o índice seja resetado aqui mesmo
        # O yfinance retorna DataFrame com Date no índice
        if df.index.name or 'Date' in str(df.index.name) or isinstance(df.index, pd.DatetimeIndex):
            logger.debug(f"  Index name para {ticker}: {df.index.name}")
            df = df.reset_index()
            logger.debug(f"  Colunas após reset para {ticker}: {list(df.columns)}")
        
        df['Ticker'] = ticker.replace('.SA', '')
        logger.info(f"✓ {ticker}: {len(df)} registros coletados")
    else:
        logger.warning(f"✗ {ticker}: sem dados disponíveis")
    
    return df


def get_combined_data(tickers: list, period: str = "1y") -> pd.DataFrame:
    """Coleta e combina dados de múltiplas ações"""
    all_data = []
    
    logger.info(f"\nColetando {len(tickers)} ações...")
    
    for ticker in tickers:
        try:
            df = get_stock_data(ticker, period)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.error(f"✗ Erro em {ticker}: {str(e)}")
    
    if all_data:
        # Concatenar todos os DataFrames (já vêm com reset_index feito)
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"\n✓ Total: {len(combined_df)} registros de {len(all_data)} ações")
        
        logger.debug(f"Colunas após concat: {list(combined_df.columns)}")
        
        # Renomeia as colunas para ficarem em lowercase e sem espaço
        combined_df.columns = [col.lower().replace(" ", "-") for col in combined_df.columns]
        
        logger.debug(f"Colunas após lowercase: {list(combined_df.columns)}")
        
        # Validar que a coluna 'date' existe
        if 'date' not in combined_df.columns:
            logger.warning(f"AVISO: Coluna 'date' não encontrada após lowercase!")
            logger.warning(f"Colunas disponíveis: {list(combined_df.columns)}")
            
            # Tentar renomear de outras possibilidades
            date_candidates = ['index', 'datetime', 'timestamp']
            renamed = False
            for candidate in date_candidates:
                if candidate in combined_df.columns:
                    combined_df = combined_df.rename(columns={candidate: 'date'})
                    logger.info(f"✓ Renomeado '{candidate}' para 'date'")
                    renamed = True
                    break
            
            if not renamed:
                logger.error(f"ERRO: Coluna 'date' não encontrada! Colunas: {list(combined_df.columns)}")
                raise ValueError("Falha ao criar coluna 'date' - verifique estrutura do yfinance")
        else:
            logger.info(f"✓ Coluna 'date' confirmada")
        
        logger.debug(f"Colunas finais: {list(combined_df.columns)}")
        
        # Cria a coluna de data de processamento "dataproc" no formato "yyyyMMdd"
        dataproc = pd.Timestamp.now().strftime("%Y%m%d")
        combined_df['dataproc'] = dataproc
        
        return combined_df
    else:
        logger.error("Nenhum dado foi coletado. Abortando job.")
        return pd.DataFrame()

if __name__ == "__main__":
    # Obter argumentos do job
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    # Inicializar Spark e Glue Context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    logger.info(f"=== Iniciando coleta de dados das ações do Ibovespa ===")
    
    # Ações populares da B3
    acoes = [
        'PETR4',  # Petrobras
        'VALE3',  # Vale
        'ITUB4',  # Itaú
        'BBDC4',  # Bradesco
        'ABEV3',  # Ambev
        'BBAS3',  # Banco do Brasil
        'MGLU3',  # Magazine Luiza
        'WEGE3',  # WEG
        'B3SA3',  # B3
        'RENT3',  # Localiza
    ]
    
    # Coleta dados do último mês usando pandas
    logger.info("Coletando dados com yfinance...")
    df_pandas = get_combined_data(acoes, period='1mo')
    
    if df_pandas.empty:
        logger.error("Nenhum dado foi coletado. Abortando job.")
        job.commit()
        sys.exit(1)
    
    # Validar estrutura do DataFrame Pandas antes de converter
    logger.info(f"Total de registros coletados: {len(df_pandas)}")
    logger.info(f"Colunas do DataFrame Pandas: {list(df_pandas.columns)}")
    logger.info(f"Tipos das colunas: {df_pandas.dtypes.to_dict()}")
    
    # Verificar se coluna 'date' existe
    if 'date' not in df_pandas.columns:
        logger.error(f"ERRO CRÍTICO: Coluna 'date' não existe no DataFrame Pandas!")
        logger.error(f"Colunas disponíveis: {list(df_pandas.columns)}")
        logger.error(f"Primeiras 5 linhas:\n{df_pandas.head()}")
        raise ValueError("Coluna 'date' não foi criada corretamente no collector")
    
    logger.info("✓ Validação: Coluna 'date' presente no DataFrame Pandas")
    logger.info(f"Amostra dos dados coletados (5 primeiras linhas):\n{df_pandas.head().to_string()}")
    
    # Converter pandas DataFrame para Spark DataFrame
    logger.info(f"Convertendo {len(df_pandas)} registros para Spark DataFrame...")
    df = spark.createDataFrame(df_pandas)
    
    # Validar schema do Spark DataFrame
    logger.info("Schema do Spark DataFrame:")
    df.printSchema()
    logger.info(f"Colunas no Spark DataFrame: {df.columns}")
        
    # diretorio S3 para salvar os dados
    raw_s3_path = 's3://fiap-etl-367813/raw/'
    logger.info(f"Salvando dados em: {raw_s3_path}")
    
    #salvar os dados em formato parquet, particionando por data de processamento
    df.write.mode("overwrite").partitionBy("dataproc").parquet(raw_s3_path)
    logger.info(f"Dados salvos com sucesso no S3: {df.count()} registros")
    
    #cliente do boto3 para glue
    glue_client = boto3.client('glue')
    logger.info(f"Cliente do boto3 para glue criado com sucesso")
    database_name = 'b3_data'
    table_name = 'stocks'
    table_location = raw_s3_path
    logger.info(f"Database: {database_name}, Tabela: {table_name}, Localização: {table_location}")
    
    # definição da tabela com nomes de colunas corretos (lowercase com hífen)
    table_input = {
        'Name': table_name,
        'Description': 'Dados históricos de ações do Ibovespa com granularidade diária',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'open', 'Type': 'double', 'Comment': 'Preço de abertura'},
                {'Name': 'high', 'Type': 'double', 'Comment': 'Preço máximo do dia'},
                {'Name': 'low', 'Type': 'double', 'Comment': 'Preço mínimo do dia'},
                {'Name': 'close', 'Type': 'double', 'Comment': 'Preço de fechamento'},
                {'Name': 'volume', 'Type': 'bigint', 'Comment': 'Volume negociado'},
                {'Name': 'dividends', 'Type': 'double', 'Comment': 'Dividendos pagos'},
                {'Name': 'stock-splits', 'Type': 'double', 'Comment': 'Desdobramentos de ações'},
                {'Name': 'ticker', 'Type': 'string', 'Comment': 'Símbolo do ativo'},
            ],
            'Location': table_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {
                    'serialization.format': '1'
                }
            }
        },
        'PartitionKeys': [
            {'Name': 'dataproc', 'Type': 'string', 'Comment': 'Data de processamento no formato yyMMdd'},
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE',
            'parquet.compression': 'SNAPPY',
        }
    }
    
    # cria ou atualiza a tabela no glue data catalog
    try:
        glue_client.get_table(DatabaseName=database_name, Name=table_name)
        logger.info(f"Tabela '{table_name}' já existe, atualizando...")
        glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
        logger.info(f"Tabela '{table_name}' atualizada com sucesso")
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            logger.info(f"Tabela '{table_name}' não existe, criando...")
            glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
            logger.info(f"Tabela '{table_name}' criada com sucesso")        
        else:
            raise
    
    logger.info(f"Tabela '{table_name}' processada e salva no S3 e no Glue Data Catalog com sucesso")
    
    # executar MSCK REPAIR TABLE para descobrir partições
    try:
        query_string = f"MSCK REPAIR TABLE {database_name}.{table_name}"
        logger.info(f"Executando MSCK REPAIR TABLE para descobrir partições: {query_string}")
        spark.sql(query_string)
        logger.info(f"MSCK REPAIR TABLE executado com sucesso")
        logger.info(f"Partições descobertas e adicionadas com sucesso")
    except Exception as e:
        logger.warning(f"Erro ao executar MSCK REPAIR TABLE: {str(e)}")
        logger.info("Continuando mesmo assim (partições podem ser adicionadas manualmente)")
    
    logger.info("=== Job concluído com sucesso ===")
    job.commit()