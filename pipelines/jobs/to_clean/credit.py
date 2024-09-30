from utils.db_conn import PostgreSQL
from datetime import datetime
import pandas as pd


def standardization(df):
    df_clean = pd.DataFrame()

    df_clean['id'] = df['id']
    df_clean['id_parcel'] = df['id_parcel']
    df_clean['id_classification'] = df['id_classification']
    df_clean["dt_compra"] = pd.to_datetime(df["Data de Compra"], format="%d/%m/%Y")
    df_clean["dt_competencia"] = pd.to_datetime(df['Data de Compra'], format='%d/%m/%Y').dt.to_period('M').dt.to_timestamp()
    df_clean['nm_cartao'] = df['Nome no Cartão']
    df_clean['final_cartao'] = df["Final do Cartão"]
    df_clean['categoria_cartao'] = df['Categoria']
    df_clean[['parcela_numerador', 'parcela_denominador']] = df['Parcela'].str.replace('Única', '1/1').str.split('/', expand=True)
    df_clean['descricao'] = df['Descrição']
    df_clean['valor_dollar'] = df['Valor (em US$)'].astype(float)
    df_clean['cotacao_real'] = df['Cotação (em R$)'].astype(float)
    df_clean['valor_real'] = df['Valor (em R$)'].astype(float)
    df_clean['time'] = datetime.now()
    df_clean['source'] = df['source']
    df_clean['file_name'] = df['file_name']

    return df_clean


def etl():
    print("Iniciando o job to clean")
    psql = PostgreSQL()

    df = psql.query("SELECT * FROM raw.c6_credit")
    print(f'{len(df)} retornaram da camada raw')

    df_clean = standardization(df)
    print(df_clean.info())

    psql.create_table("""
        -- DROP TABLE clean.c6_credit;
        CREATE TABLE IF NOT EXISTS clean.c6_credit (
            id TEXT PRIMARY KEY,
            id_parcel TEXT,
            id_classification TEXT,
            dt_compra DATE,
            dt_competencia DATE,
            nm_cartao TEXT,
            final_cartao TEXT,
            categoria_cartao TEXT,
            descricao TEXT,
            parcela_numerador TEXT,
            parcela_denominador TEXT,
            valor_dollar NUMERIC,
            cotacao_real NUMERIC,
            valor_real NUMERIC,
            time TIMESTAMP,
            source TEXT,
            file_name TEXT
        );
    """)

    psql.insert_from_pandas(
        schema='clean',
        table_name='c6_credit',
        df=df_clean
    )


if __name__ == "__main__":
    etl()
