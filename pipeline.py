# -*- coding: utf-8 -*-
"""
Conecta no Open-Mateo via API
"""
# Importações
# pipeline.py
import os
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, Numeric, MetaData, Table, create_engine
from sqlalchemy.dialects.postgresql import insert



# ----------- Configurações globais -------------------------------------------------
 # Carrega variáveis de ambiente do .env
from pathlib import Path
load_dotenv(Path(__file__).resolve().parent / ".env")

# Busca a string de conexão
PG_URI = os.getenv("PG_URI")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Configura o log
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Cria engine com a URI lida
engine = create_engine(PG_URI)
  
  
#------------------ Parâmetros de tempo para Chapecó - SC ---------------------
LAT = -27.0964     
LON = -52.6183
HOURLY_VARS = [
    "temperature_2m",                   # temperatura
    "rain",                             # milímetros acumulados na hora anterior
    "visibility",                       # metros (“quantos metros à frente você enxerga”)
    "precipitation_probability"         #  % de chance de cair > 0,1 mm na hora
]

url = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={LAT}&longitude={LON}"
    f"&hourly={','.join(HOURLY_VARS)}"
    # deixa os horários já em America/Sao_Paulo
    "&timezone=auto"           
)

#Busca dados de tempo da API
def extrair():
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()  # Gera mensagem de erro se a API falhar
    dados = resp.json()["hourly"]
    return pd.DataFrame(dados)

#------------------------------------------------------------------------------
def transformar(df):
    df["time"] = pd.to_datetime(df["time"])
    num_cols = ["temperature_2m", "rain", "visibility", "precipitation_probability"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    # opcional – tratar nulos
    df["rain"] = df["rain"].fillna(0)                           # sem chuva vira 0 mm
    df["visibility"] = df["visibility"].fillna(method="ffill")  # propaga último valor

    return df

#-------- Criando a tabela para gravar o dado da API --------------------------
metadata = MetaData()

clima_chapeco = Table(
    "clima_chapeco",
    metadata,
    Column("time",  DateTime, primary_key=True),
    Column("temperature_2m", Numeric(4,1)),
    Column("rain",  Numeric(5,2)),
    Column("visibility", Numeric(8,0)),
    Column("precipitation_probability", Numeric(3,0))
)

metadata.create_all(engine) # cria a tabela com PK se não existir

# Salva os dados no PostgreSQL
def carregar(df):  

    # garante datetime em formato Python puro
    df["time"] = df["time"].dt.to_pydatetime()

    registros = df.to_dict(orient="records")

    stmt = insert(clima_chapeco).values(registros)
   
    stmt = stmt.on_conflict_do_update(
    index_elements=["time"],
    set_={
        # coluna : valor_atualizado
        "temperature_2m": stmt.excluded.temperature_2m,
        "rain": stmt.excluded.rain,
        "visibility": stmt.excluded.visibility,
        "precipitation_probability": stmt.excluded.precipitation_probability,
        }
    )

    with engine.begin() as conn:   # transação atômica
        conn.execute(stmt)


if __name__ == "__main__":
    try:
        df = extrair()
        df = transformar(df)
        carregar(df)
        logging.info("Pipeline executado com sucesso.")
    except Exception:
        logging.exception("Pipeline falhou.")