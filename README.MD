# Olá!! Seja bem-vindo(a) ao meu pequeno projeto de ETL em PySpark, espero que goste :D

## Requisitos:
- PySpark
- Docker - Docker Compose
- MySQL
- Matplotlib
- Pandas
- Seaborn

## Estrutura:
- **Pasta BI**: [Arquivos do Power BI]
- **Factory**: [Tabelas intermediárias]
- **Staging**: [Arquivos temporários pré manipulação]
- **.env.ex**: [Exemplo de envvars]
- **Arquivos docker**: [Instruções para inicialização do container]
- **etl.py**: [Arquivo de ETL]
- **exploratory.ipynb**: [Análise com PySpark/Pandas e visualização com Seaborn e Matplotlib]

## Como iniciar:

1. Configure suas variáveis de ambiente no arquivo `.env` (veja `.env.ex`).
2. Inicie o Docker: `docker-compose up`.
3. Execute as etapas do notebook.

## Observações:
Os logins e senhas no arquivo `.env` destinam-se apenas para realização de testes, não use em casos reais.
