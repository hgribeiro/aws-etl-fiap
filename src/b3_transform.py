"""
Script para transformar dados de ações da B3 - Raw para Refined (AWS Glue)
TRANSFORMAÇÕES OBRIGATÓRIAS DO PROJETO:
A) Agrupamento numérico, sumarização, contagem e soma
B) Renomear duas colunas existentes
C) Cálculo com base na data (diferença entre períodos)
"""
import sys
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, to_date, lag, avg,
    min as spark_min, max as spark_max, count, sum as spark_sum,
    round as spark_round, when, year, month
)
import boto3
from botocore.exceptions import ClientError

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def prepare_data(df: DataFrame) -> DataFrame:
    """
    Prepara os dados básicos (conversão de tipos e colunas de data)
    """
    logger.info("Preparando dados...")
    
    # Garantir que date está no formato correto
    df = df.withColumn("date", to_date(col("date")))
    
    # Criar colunas de data (year, month) para agrupamento
    df = df \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date")))
    
    logger.info("Dados preparados com sucesso")
    return df


def apply_mandatory_transformations(df: DataFrame) -> DataFrame:
    """
    TRANSFORMAÇÕES OBRIGATÓRIAS DO PROJETO:
    B: Renomear duas colunas existentes além das de agrupamento
    C: Realizar cálculo com base na data (diferença entre períodos)
    """
    logger.info("=== INICIANDO TRANSFORMAÇÕES OBRIGATÓRIAS (B e C) ===")
    
    # B) RENOMEAR DUAS COLUNAS - Requisito Obrigatório
    logger.info("B) Renomeando colunas conforme requisito...")
    df_renamed = df \
        .withColumnRenamed("close", "preco_fechamento") \
        .withColumnRenamed("volume", "volume_negociado")
    logger.info("✓ Colunas renomeadas: 'close' -> 'preco_fechamento', 'volume' -> 'volume_negociado'")
    
    # C) CÁLCULO COM BASE NA DATA - Diferença entre períodos
    logger.info("C) Calculando diferença de preço entre períodos (7 e 30 dias)...")
    window_spec = Window.partitionBy("ticker").orderBy("date")
    
    df_date_calc = df_renamed \
        .withColumn("preco_7d_atras", lag("preco_fechamento", 7).over(window_spec)) \
        .withColumn("preco_30d_atras", lag("preco_fechamento", 30).over(window_spec)) \
        .withColumn("variacao_7d", 
                   when(col("preco_7d_atras").isNotNull(),
                        spark_round((col("preco_fechamento") - col("preco_7d_atras")) / col("preco_7d_atras") * 100, 2))
                   .otherwise(None)) \
        .withColumn("variacao_30d",
                   when(col("preco_30d_atras").isNotNull(),
                        spark_round((col("preco_fechamento") - col("preco_30d_atras")) / col("preco_30d_atras") * 100, 2))
                   .otherwise(None))
    logger.info("✓ Calculadas: variacao_7d e variacao_30d (diferença percentual entre períodos)")
    
    logger.info("=== TRANSFORMAÇÕES OBRIGATÓRIAS (B e C) CONCLUÍDAS ===")
    return df_date_calc


def create_aggregated_summary(df: DataFrame) -> DataFrame:
    """
    A) AGRUPAMENTO NUMÉRICO - Requisito Obrigatório
    Cria sumário agregado por ticker e mês com contagens e somas
    """
    logger.info("=== A) CRIANDO AGRUPAMENTO NUMÉRICO (SUMARIZAÇÃO) ===")
    
    # Agrupamento por ticker, ano e mês com múltiplas agregações
    df_summary = df.groupBy("ticker", "year", "month") \
        .agg(
            count("*").alias("total_registros"),
            spark_sum("volume_negociado").alias("volume_total_mes"),
            spark_round(avg("preco_fechamento"), 2).alias("preco_medio_mes"),
            spark_round(spark_min("preco_fechamento"), 2).alias("preco_minimo_mes"),
            spark_round(spark_max("preco_fechamento"), 2).alias("preco_maximo_mes"),
            spark_round(avg("variacao_7d"), 2).alias("variacao_7d_media"),
            spark_round(avg("variacao_30d"), 2).alias("variacao_30d_media")
        ) \
        .orderBy("ticker", "year", "month")
    
    logger.info("✓ Agrupamento criado: ticker + year + month")
    logger.info("✓ Agregações: contagem, soma, média, mínimo e máximo")
    logger.info("=== AGRUPAMENTO NUMÉRICO CONCLUÍDO ===")
    
    return df_summary


def filter_null_values(df: DataFrame) -> DataFrame:
    """
    Remove registros com valores nulos nas colunas essenciais
    """
    logger.info("Filtrando valores nulos...")
    
    # Remove registros com valores nulos críticos
    df_clean = df \
        .filter(col("close").isNotNull()) \
        .filter(col("volume").isNotNull()) \
        .filter(col("ticker").isNotNull()) \
        .filter(col("date").isNotNull())
    
    initial_count = df.count()
    final_count = df_clean.count()
    removed_count = initial_count - final_count
    
    if removed_count > 0:
        logger.warning(f"Removidos {removed_count} registros com valores nulos")
    
    logger.info(f"Dados filtrados: {final_count} registros válidos")
    return df_clean


def create_glue_table(glue_client, database_name: str, table_name: str, table_location: str):
    """
    Cria ou atualiza tabela no Glue Data Catalog - APENAS TRANSFORMAÇÕES OBRIGATÓRIAS
    Particionado por data (dataproc) e ticker (código da ação)
    """
    logger.info(f"Criando/atualizando tabela {database_name}.{table_name}...")
    
    table_input = {
        'Name': table_name,
        'Description': 'Dados transformados de ações da B3 - Particionado por data e ticker',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'date', 'Type': 'date', 'Comment': 'Data da cotação'},
                {'Name': 'open', 'Type': 'double', 'Comment': 'Preço de abertura'},
                {'Name': 'high', 'Type': 'double', 'Comment': 'Preço máximo'},
                {'Name': 'low', 'Type': 'double', 'Comment': 'Preço mínimo'},
                {'Name': 'preco_fechamento', 'Type': 'double', 'Comment': '[B-RENOMEADO] Preço de fechamento'},
                {'Name': 'volume_negociado', 'Type': 'bigint', 'Comment': '[B-RENOMEADO] Volume negociado'},
                {'Name': 'dividends', 'Type': 'double', 'Comment': 'Dividendos'},
                {'Name': 'stock-splits', 'Type': 'double', 'Comment': 'Desdobramentos'},
                {'Name': 'year', 'Type': 'int', 'Comment': 'Ano (para agrupamento)'},
                {'Name': 'month', 'Type': 'int', 'Comment': 'Mês (para agrupamento)'},
                {'Name': 'preco_7d_atras', 'Type': 'double', 'Comment': '[C-CALC DATA] Preço 7 dias atrás'},
                {'Name': 'preco_30d_atras', 'Type': 'double', 'Comment': '[C-CALC DATA] Preço 30 dias atrás'},
                {'Name': 'variacao_7d', 'Type': 'double', 'Comment': '[C-CALC DATA] Variação % 7 dias'},
                {'Name': 'variacao_30d', 'Type': 'double', 'Comment': '[C-CALC DATA] Variação % 30 dias'},
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
            {'Name': 'dataproc', 'Type': 'string', 'Comment': 'Data de processamento (yyyyMMdd)'},
            {'Name': 'ticker', 'Type': 'string', 'Comment': 'Código da ação/índice'},
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE',
            'parquet.compression': 'SNAPPY',
        }
    }
    
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
            logger.error(f"Erro ao verificar/criar tabela: {str(e)}")
            raise


if __name__ == "__main__":
    # Obter parâmetros do job
    try:
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME', 
            'S3_BUCKET', 
            'DATABASE_NAME',
            'TARGET_TABLE'
        ])
        s3_bucket = args['S3_BUCKET']
        database_name = args['DATABASE_NAME']
        target_table = args['TARGET_TABLE']
    except:
        # Valores padrão
        logger.warning("Usando valores padrão para parâmetros")
        s3_bucket = 'fiap-etl-367813'
        database_name = 'b3_data'
        target_table = 'stocks_refined'
        args = {'JOB_NAME': 'b3_transform'}
    
    # Inicializar Spark e Glue Context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    logger.info("=== Iniciando transformação de dados B3 (Raw -> Refined) ===")
    logger.info(f"Bucket S3: {s3_bucket}")
    logger.info(f"Database: {database_name}")
    logger.info(f"Tabela destino: {target_table}")
    
    # Caminhos S3
    raw_s3_path = f's3://{s3_bucket}/raw/'
    refined_s3_path = f's3://{s3_bucket}/refined/'
    
    # Ler dados da camada raw
    logger.info(f"Lendo dados de: {raw_s3_path}")
    try:
        df_raw = spark.read.parquet(raw_s3_path)
        record_count = df_raw.count()
        logger.info(f"Lidos {record_count} registros da camada raw")
        
        # Log do schema para debug
        logger.info("Schema dos dados raw:")
        df_raw.printSchema()
        logger.info(f"Colunas disponíveis: {df_raw.columns}")
        
        # Mostrar amostra dos dados
        logger.info("Amostra dos dados raw (5 primeiras linhas):")
        df_raw.show(5, truncate=False)
        
        if record_count == 0:
            logger.warning("Nenhum dado encontrado na camada raw")
            job.commit()
            sys.exit(0)
    except Exception as e:
        logger.error(f"Erro ao ler dados raw: {str(e)}")
        raise
    
    # Aplicar APENAS transformações obrigatórias
    logger.info("Iniciando pipeline de transformação (APENAS OBRIGATÓRIAS)...")
    
    try:
        # 1. Filtrar valores nulos
        logger.info("ETAPA 1: Filtrando valores nulos...")
        df_filtered = filter_null_values(df_raw)
        logger.info(f"Dados filtrados: {df_filtered.count()} registros")
        
        # 2. Preparar dados (conversões de tipo e colunas de data)
        logger.info("ETAPA 2: Preparando dados...")
        df_prepared = prepare_data(df_filtered)
        logger.info(f"Dados preparados: {df_prepared.count()} registros")
        
        # 3. APLICAR TRANSFORMAÇÕES OBRIGATÓRIAS (B e C)
        logger.info("ETAPA 3: Aplicando transformações obrigatórias (B e C)...")
        df_final = apply_mandatory_transformations(df_prepared)
        logger.info(f"Transformações obrigatórias aplicadas: {df_final.count()} registros")
        
    except Exception as e:
        logger.error(f"ERRO na transformação: {str(e)}")
        logger.error(f"Tipo de erro: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback completo:\n{traceback.format_exc()}")
        raise
    
    # Estatísticas finais
    final_count = df_final.count()
    logger.info(f"Transformação concluída: {final_count} registros processados")
    
    # Mostrar amostra dos dados transformados
    logger.info("Amostra dos dados transformados (TRANSFORMAÇÕES OBRIGATÓRIAS):")
    df_final.select(
        "date", "ticker", "preco_fechamento", "volume_negociado",
        "variacao_7d", "variacao_30d", "year", "month"
    ).show(10, truncate=False)
    
    # A) CRIAR SUMÁRIO AGREGADO PARA DEMONSTRAÇÃO - Requisito Obrigatório
    logger.info("ETAPA 4: Criando sumário agregado (AGRUPAMENTO NUMÉRICO - A)...")
    df_summary = create_aggregated_summary(df_final)
    logger.info("Amostra do sumário agregado (demonstração do requisito A):")
    df_summary.show(15, truncate=False)
    
    # Salvar dados transformados na camada refined (TABELA ÚNICA)
    # Particionado por data (dataproc) e ticker (código da ação)
    logger.info(f"Salvando dados transformados em: {refined_s3_path}")
    logger.info("Particionamento: dataproc (data) e ticker (código da ação)")
    df_final.write \
        .mode("overwrite") \
        .partitionBy("dataproc", "ticker") \
        .parquet(refined_s3_path)
    
    logger.info(f"Dados salvos com sucesso: {df_final.count()} registros")
    logger.info("Estrutura de pastas: refined/dataproc=YYYYMMDD/ticker=XXXX/")
    
    # Criar/atualizar tabela no Glue Data Catalog (TABELA ÚNICA)
    glue_client = boto3.client('glue')
    
    # Tabela principal de dados transformados
    create_glue_table(glue_client, database_name, target_table, refined_s3_path)
    logger.info(f"Tabela criada/atualizada: {target_table}")
    
    # Executar MSCK REPAIR TABLE para descobrir partições
    try:
        query_string = f"MSCK REPAIR TABLE {database_name}.{target_table}"
        logger.info(f"Executando MSCK REPAIR TABLE: {query_string}")
        spark.sql(query_string)
        logger.info("Partições descobertas e adicionadas com sucesso")
    except Exception as e:
        logger.warning(f"Erro ao executar MSCK REPAIR TABLE: {str(e)}")
        logger.info("Continuando mesmo assim (partições podem ser adicionadas manualmente)")
    
    # Estatísticas por ticker (demonstrando agrupamento numérico - requisito A)
    logger.info("=== Estatísticas por Ticker (DEMONSTRAÇÃO DO REQUISITO A) ===")
    df_final.groupBy("ticker") \
        .agg(
            count("*").alias("total_registros"),
            spark_round(avg("preco_fechamento"), 2).alias("preco_medio"),
            spark_round(avg("variacao_7d"), 2).alias("variacao_7d_media"),
            spark_round(avg("variacao_30d"), 2).alias("variacao_30d_media")
        ) \
        .orderBy("ticker") \
        .show(20, truncate=False)
    
    logger.info("\n" + "="*70)
    logger.info("RESUMO DAS TRANSFORMAÇÕES OBRIGATÓRIAS APLICADAS")
    logger.info("="*70)
    logger.info("✓ A) AGRUPAMENTO NUMÉRICO:")
    logger.info("    - Agrupamento por: ticker + year + month")
    logger.info("    - Operações: CONTAGEM, SOMA, MÉDIA, MÍNIMO, MÁXIMO")
    logger.info(f"    - Sumarização demonstrada com {df_summary.count()} grupos")
    logger.info("    - Os dados podem ser agregados via consulta SQL na tabela única")
    logger.info("")
    logger.info("✓ B) RENOMEAR COLUNAS:")
    logger.info("    - 'close' → 'preco_fechamento'")
    logger.info("    - 'volume' → 'volume_negociado'")
    logger.info("")
    logger.info("✓ C) CÁLCULO COM BASE NA DATA:")
    logger.info("    - variacao_7d: variação % em relação a 7 dias atrás")
    logger.info("    - variacao_30d: variação % em relação a 30 dias atrás")
    logger.info("    - Fórmula: ((preco_atual - preco_anterior) / preco_anterior) * 100")
    logger.info("")
    logger.info(f"📊 TABELA ÚNICA SALVA: {database_name}.{target_table}")
    logger.info(f"📁 Localização S3: {refined_s3_path}")
    logger.info("="*70 + "\n")
    logger.info("\n💡 CONSULTAS SQL DE EXEMPLO PARA O ATHENA:")
    logger.info("="*70)
    logger.info(f"-- Dados detalhados:")
    logger.info(f"SELECT * FROM {database_name}.{target_table} LIMIT 10;")
    logger.info("")
    logger.info(f"-- Sumarização mensal (Requisito A):")
    logger.info(f"SELECT ticker, year, month,")
    logger.info(f"       COUNT(*) as total_registros,")
    logger.info(f"       SUM(volume_negociado) as volume_total,")
    logger.info(f"       ROUND(AVG(preco_fechamento), 2) as preco_medio")
    logger.info(f"FROM {database_name}.{target_table}")
    logger.info(f"GROUP BY ticker, year, month")
    logger.info(f"ORDER BY ticker, year, month;")
    logger.info("="*70 + "\n")
    
    logger.info("=== Transformação concluída com sucesso ===")
    job.commit()
