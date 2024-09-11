import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
import os

print('Iniciando ETL')
#Localização da base
base_i= 'ETL\staging\DADOS\MICRODADOS_ENEM_2020.csv'

print('Inicializando spark')
spark = SparkSession.builder \
    .appName("ETL_SX") \
    .getOrCreate()

print('Iniciando ETL')
df = spark.read.csv(base_i, header=True, inferSchema=True, sep=';')


# Declaração de colunas úteis

cols = [ 'NU_INSCRICAO', 'NU_ANO', 'TP_FAIXA_ETARIA', 'TP_SEXO', 'TP_COR_RACA', 
 'TP_ESCOLA', 'TP_PRESENCA_CN', 'TP_PRESENCA_CH', 
 'TP_PRESENCA_LC', 'TP_PRESENCA_MT', 'CO_PROVA_CN', 'CO_PROVA_CH', 'CO_PROVA_LC', 
 'CO_PROVA_MT', 'NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO', 'Q006']


df = df.select(*cols)

# Esta etapa visa remover as linhas duplicadas do dataframe
df = df.dropDuplicates()

# Inserção de valor None em células vazias 
for col_name in cols:
    df = df.withColumn(col_name, when(col(col_name) == '', None).otherwise(col(col_name)))

# Filtro para remover linhas com notas inferiores a zero, utilizada pra agilizar os testes com uma base menor
#df = df.filter(
#    (col('NU_NOTA_CN') < 1) & 
#    (col('NU_NOTA_CH') < 1) & 
#    (col('NU_NOTA_LC') < 1) & 
#    (col('NU_NOTA_MT') < 1)
#)

# Diretório de saída do arquivo de backup
fdir= r'BI\base'

# Escrita do arquivo
df.coalesce(1).write.csv(fdir, header=True, sep=';', mode='overwrite')

# Renomeando arquivo de backup
# Em caso de execuções automatizadas e sequenciais recomenda-se a deleção ou movimetação do arquivo de bakup antigo
#os.sys(r'mv BI\base\*csv BI\base\ENEM_2020..csv')

# Configurações de conexão com o MySQL
jdbc_url = "jdbc:mysql://localhost:3306/ENEM_2020"
connection_properties = {
    "user": "root",
    "password": "admin",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Escreve o DataFrame para a tabela 'ENEM_2020' no MySQL
df.write.jdbc(url=jdbc_url, table="ENEM_2020", mode="overwrite", properties=connection_properties)
print('ETL concluido')

