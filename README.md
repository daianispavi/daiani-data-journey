# daiani-data-journey

# Sprint 1 - ETL com Python + PostgreSQL

Este projeto faz parte da minha jornada de atualização para Engenharia de Dados Moderno.  
A Sprint 1 consiste em um pipeline simples que coleta dados meteorológicos da API [Open-Meteo](https://open-meteo.com/), transforma os dados com pandas e carrega no banco PostgreSQL local.

## Tecnologias utilizadas

- Python
- pandas
- requests
- PostgreSQL
- SQLAlchemy

### Configurações sensíveis (.env)

Crie um arquivo **.env** na raiz do projeto, copiando o template **.env.example**, e preencha:

PG_URI=postgresql://pipeline_data:<SUA_SENHA>@localhost:5432/pipeline_db
LOG_LEVEL=INFO

> Não versione o '.env'. Ele já está no '.gitignore'.

## Como executar

### Criar banco e rodar pipeline

# 1. Instalar dependências
pip install -r requirements.txt

# 2. Criar usuário e banco (uma vez)
psql -U postgres -c "CREATE DATABASE pipeline_db;"
psql -U postgres -c "CREATE USER pipeline_data WITH PASSWORD 'senha';"
psql -U postgres -d pipeline_db -c "GRANT ALL ON DATABASE pipeline_db TO pipeline_data;"

# 3. Preencher .env
PG_URI=postgresql://pipeline_data:senha@localhost:5432/pipeline_db

# 4. Executar
python pipeline.py

## Resultado

Print abaixo mostra os dados carregados na tabela 'clima_chapeco' via pgAdmin.

![Tabela populada](assets/dados_postgres.png)

## Agendamento automático

Este pipeline está configurado para rodar automaticamente todos os dias às **8h da manhã** por meio do **Agendador de Tarefas do Windows**.

### Como funciona:

Um arquivo 'rodar_pipeline.bat' foi criado com o seguinte conteúdo:

```bat
@echo off
cd /d C:\<CAMINHO_DO_SEU_PROJETO>
python pipeline.py
