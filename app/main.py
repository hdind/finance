from dotenv import load_dotenv
import os


def main():
	load_dotenv()

	ROOT = os.getenv('ROOT')

	folder = f'{ROOT}/src/faturas'

	archives = os.listdir(folder)
	paths = [os.path.join(folder, archive) for archive in archives]

	for path in paths:
		print(f'Starting process to raw for {path.split("/")[-1]}')
		os.system(f'python {ROOT}/app/to_raw/faturas.py {path}')
		
		os.system(f'mv {path} {folder}/readed/')
		print(f'{path.split("/")[-1]} moved to `readed` folder')
		
	print('All success!')


if __name__ == '__main__':
	main()
