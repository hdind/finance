from datetime import datetime
from dotenv import load_dotenv

import pandas as pd
import hashlib
import sys
import sqlite3


def _generate_md5(*args) -> str:
	concat = ''.join(args)
	return hashlib.md5(concat.encode()).hexdigest()

def main():
	if len(sys.argv) != 2:
		print('Faltou o parâmeto de file_path!')
	else:
		file_path = sys.argv[1]

	load_dotenv()

	ROOT = os.getenv('ROOT')

	file_name = file_path.split('/')[-1]

	print(f'Reading the {file_name}')
	df = pd.read_csv(file_path, header=0, sep=';', dtype='str')

	print('Creating metadatas')
	df['_time'] = datetime.now()

	df['_md5_parcel'] = df.apply(lambda row: _generate_md5(
		row['Data de Compra'],
		row['Final do Cartão'],
		row['Descrição']
		), axis=1)

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

	table_name = 'c6_faturas'
	db_name = 'finance_raw.db'
	print(f'Created df has {qtd_rows} and will be appended at {table_name}')

	with sqlite3.connect(f'{ROOT}/databases/{db_name}') as conn:
		df.to_sql(table_name, conn, if_exists='append', index=False)

	print('The append has executed successfully on {db_name}.{table_name}')

if __name__ == '__main__':
	main()