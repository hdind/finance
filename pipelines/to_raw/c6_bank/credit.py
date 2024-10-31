from configs.config import ROOT
from utils.db_conn import PostgreSQL
from utils.util import FileManager, add_metadata
from datetime import datetime
import pandas as pd


def load_credit_to_raw():
    print("Iniciando o job to raw")
    file_manager = FileManager(ROOT, 'credit', 'csv')

    paths_to_read = file_manager.list_paths_to_read()
    print(f'{len(paths_to_read)} arquivos para serem lidos')

    now = datetime.now()
    print(now)

    df_raw = pd.DataFrame()
    for path in paths_to_read:
        df_temp = pd.read_csv(path, header=0, sep=';', dtype='str')
        print(f'+{len(df_temp)} linhas no df final')
        df_temp = add_metadata(df_temp, path, now, 'credit')

        df_raw = pd.concat([df_raw, df_temp])

    psql = PostgreSQL()

    psql.create_table("""
        -- DROP TABLE raw.c6_credit;
        CREATE TABLE IF NOT EXISTS raw.c6_credit (
            id TEXT PRIMARY KEY,
            "Data de Compra" TEXT,
            "Nome no Cartão" TEXT,
            "Final do Cartão" TEXT,
            "Categoria" TEXT,
            "Descrição" TEXT,
            "Parcela" TEXT,
            "Valor (em US$)" TEXT,
            "Cotação (em R$)" TEXT,
            "Valor (em R$)" TEXT,
            time TIMESTAMP,
            source TEXT,
            file_name TEXT
        );
    """)

    psql.insert_from_pandas(
        schema="raw",
        table_name="c6_credit",
        df=df_raw
    )

    file_manager.move_readed_files()


if __name__ == "__main__":
    load_credit_to_raw()
