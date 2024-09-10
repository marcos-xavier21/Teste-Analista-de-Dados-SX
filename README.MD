Olá!! Seja bem vindo(a) ao meu pequeno projeto de ETL em Pyspark, espero que goste :D

Requisitos:
PySpark
Docker -  Docker Compose
MySQL
matplotlib
pandas
seaborn

Estrutura:
Pasta BI [Arquivos do Power BI]
Factory [Tabelas intermediárias]
Staging [Arquivos temporários pré maniplação]]

.env.ex [Exemplo de envvars]
Arquivos docker [Instruções para inicialização do container]
etl.py [Arquivo de ETL]
exploratory.ipynb [Analise com pyspark/pandas e visualização com seaborn e matplotlib]


Como iniciar

Configure suas variáveis de ambiente no arquivo .env (veja .env.ex)
Inicie o docker: docker-compose up 
Execute as etapas do notebook

Observações:
Logins e senhas no arquivo .env destinam-se apenas para realização a de testes, não use em casos reais