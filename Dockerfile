# Dockerfile para o PySpark
FROM  apache/spark-py:latest

# Cria um diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o script do ETL e o arquivo de dependências para o diretório de trabalho
COPY main.py requirements.txt ./ 
COPY staging/DADOS/MICRODADOS_ENEM_2020.csv ./ 


# Inicializa o script Python
CMD ["sh", "-c", "python3 main.py"]
