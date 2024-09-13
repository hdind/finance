from datetime import datetime
from dotenv import load_dotenv

import fitz 
import pdfplumber
import pandas as pd
import hashlib
import sys
import sqlite3
import os


def _generate_md5(*args) -> str:
	concat = ''.join(args)
	return hashlib.md5(concat.encode()).hexdigest()

def desbloquear_pdf(pdf_path, password):
    pdf_document = fitz.open(pdf_path)
    pdf_document.authenticate(password)
    return pdf_document

def main():
	if len(sys.argv) != 3:
		print('Faltou o parâmeto de pdf_path e password')
        return 1

    else:
        pdf_path = sys.argv[1]
        password = sys.argv[2]

    load_dotenv()

	ROOT = os.getenv('ROOT')

	file_name = pdf_path.split('/')[-1]

    pdf_document = desbloquear_pdf(pdf_path, password)

    pdf_path_temp = 'temp.pdf'
    pdf_document.save(pdf_path_temp)

    df = pd.DataFrame()

    print(f'Reading the {file_name}')
    with pdfplumber.open(pdf_path_temp) as pdf:
        for i, page in enumerate(pdf.pages):
            print(f'reading page {i}')
            if i == 0:
                print('first_page')
                raw = page.crop((20, 225, page.width, 240))

                header = raw.extract_table(
                    table_settings={
                        "vertical_strategy": "explicit",
                        "horizontal_strategy": "text",
                        "explicit_vertical_lines": [30, 63, 350, 450, 485, 500, 575]
                        }
                    )
                
                print(header)
                
                raw = page.crop((20, 240, page.width, 700))
            
            else:
                raw = page.crop((20, 175, page.width, 700))

            data = raw.extract_table(
                table_settings={
                    "vertical_strategy": "explicit",
                    "horizontal_strategy": "text",
                    "explicit_vertical_lines": [30, 63, 350, 450, 485, 500, 575]
                    }
                )
            
            df_aux = pd.DataFrame(data, columns=header)
            df = pd.concat([df, df_aux])

	print('Creating metadatas')
	df['_time'] = datetime.now()

    qtd_rows = len(df)
	df['_qt_rows'] = qtd_rows

	df['_file_name'] = file_name

	df['id'] = df.apply(lambda row: _generate_md5(
		row['Data de Compra'],
		row['Nome no Cartão'],
		row['Final do Cartão'],
		row['Categoria'],
   		row['Descrição'],
   		row['Parcela'],
   		row['Valor (em US$)'],
   		row['Cotação (em R$)'],
        row['Valor (em R$)']
		), axis=1)

    table_name = 'c6_extratos'
	db_name = 'finance_raw.db'
	print(f'Created df has {qtd_rows} and will be appended at {table_name}')

	with sqlite3.connect(f'{ROOT}/databases/{db_name}') as conn:
		df.to_sql(table_name, conn, if_exists='append', index=False)

	print('The append has executed successfully on {db_name}.{table_name}')

if __name__ == "__main__":
    main()