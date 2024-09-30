from utils.db_conn import PostgreSQL
from datetime import datetime
import pandas as pd


def standardization(df):
    df_clean = pd.DataFrame()

    df_clean['id'] = df['id']
    df_clean['id_classification'] = df['id_classification']
    df_clean["dt_compra"] = pd.to_datetime(df["DATA"], format="%d/%m/%Y")
    df_clean["dt_competencia"] = pd.to_datetime(df['DATA'], format='%d/%m/%Y').dt.to_period('M').dt.to_timestamp()
    df_clean['descricao'] = df['DESCRIÇÃO']
    df_clean['doc'] = df['DOC']
    df_clean['valor_real'] = df['VALOR'].str.replace('.', '').str.replace(',', '.').astype(float)
    df_clean['d_c'] = df['D/C']
    df_clean['time'] = datetime.now()
    df_clean['source'] = df['source']
    df_clean['file_name'] = df['file_name']

    return df_clean


def etl():
    print("Iniciando o job to clean")
    psql = PostgreSQL()

    df = psql.query("""
            SELECT
                *
            FROM
                raw.c6_debit
            WHERE
                "DOC" = '000000000000';
    """)
    print(f'{len(df)} retornaram da camada raw')

    df_clean = standardization(df)
    print(df_clean.info())

    psql.create_table("""
        -- DROP TABLE clean.c6_debit;
        CREATE TABLE IF NOT EXISTS clean.c6_debit (
            id TEXT PRIMARY KEY,
            id_classification TEXT,
            dt_compra DATE,
            dt_competencia DATE,
            descricao TEXT,
            doc TEXT,
            valor_real NUMERIC,
            d_c TEXT,
            time TIMESTAMP,
            source TEXT,
            file_name TEXT
        );
    """)

    psql.insert_from_pandas(
        schema='clean',
        table_name='c6_debit',
        df=df_clean
    )


if __name__ == "__main__":
    etl()
