from configs.config import ROOT, PDF_PASSWORD
from utils.db_conn import PostgreSQL
from utils.util import FileManager, add_metadata
from datetime import datetime
import fitz
import pdfplumber
import pandas as pd


def unlock_pdf(pdf_path, senha=PDF_PASSWORD):
    pdf_document = fitz.open(pdf_path)
    pdf_document.authenticate(senha)
    return pdf_document


def struct_pdf(pdf_unlocked_path):
    df = pd.DataFrame()
    with pdfplumber.open(pdf_unlocked_path) as pdf:
        for i, page in enumerate(pdf.pages):
            print(f'reading page {i}')
            if i == 0:
                header = ['DATA', 'DESCRIÇÃO', 'DOC', 'VALOR', 'D/C']

                raw = page.crop((20, 240, page.width, 700))

            else:
                raw = page.crop((20, 175, page.width, 700))

            data = raw.extract_table(
                table_settings={
                    "vertical_strategy": "explicit",
                    "horizontal_strategy": "text",
                    "explicit_vertical_lines": [30, 63, 350, 450, 485, 500]
                    }
                )

            df_temp = pd.DataFrame(data, columns=header)
            df = pd.concat([df, df_temp])

    return df


def etl():
    print("Iniciando o job to raw")
    file_manager = FileManager(ROOT, 'debit', 'pdf')

    paths_to_read = file_manager.list_paths_to_read()
    print(f'{len(paths_to_read)} arquivos para serem lidos')

    now = datetime.now()

    df_raw = pd.DataFrame()
    for pdf_path in paths_to_read:
        pdf_document = unlock_pdf(pdf_path)

        pdf_unlocked_path = 'temp.pdf'
        pdf_document.save(pdf_unlocked_path)

        df_temp = struct_pdf(pdf_unlocked_path)
        df_temp = add_metadata(df_temp, pdf_path, now, 'debit')

        df_raw = pd.concat([df_raw, df_temp])

    print(df_raw.columns)

    psql = PostgreSQL()

    psql.create_table("""
        -- DROP TABLE raw.c6_debit;
        CREATE TABLE IF NOT EXISTS raw.c6_debit (
            id TEXT PRIMARY KEY,
            id_classification TEXT,
            "DATA" TEXT,
            "DESCRIÇÃO" TEXT,
            "DOC" TEXT,
            "VALOR" TEXT,
            "D/C" TEXT,
            time TIMESTAMP,
            source TEXT,
            file_name TEXT
        );
    """)

    psql.insert_from_pandas(
        schema="raw",
        table_name="c6_debit",
        df=df_raw
    )

    file_manager.move_readed_files()
    file_manager.delete_temp_file()


if __name__ == "__main__":
    etl()
