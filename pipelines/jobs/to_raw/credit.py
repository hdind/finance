from configs.config import ROOT
from utils.db_conn import PostgreSQL
from utils.util import FileManager, add_metadata
from datetime import datetime
import pandas as pd


def etl():
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
        df_temp = add_metadata(df_temp, path, now)

        df_raw = pd.concat([df_raw, df_temp])

    psql = PostgreSQL()

    psql.create_table("""
        CREATE TABLE IF NOT EXISTS raw.c6_credit (
            id TEXT PRIMARY KEY,
            id_parcel TEXT,
            id_payment TEXT,
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
    etl()
