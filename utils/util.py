import hashlib
import uuid
import os


def _generate_primary_id():
    return str(uuid.uuid4())


def _generate_foreign_id(*args):
    combined_string = ''.join(args)
    return hashlib.sha256(combined_string.encode()).hexdigest()


def add_metadata(df, path, now, statement):
    df['id'] = df.apply(lambda _: _generate_primary_id(), axis=1)
    df['time'] = now
    df['source'] = 'C6 credit'
    df['file_name'] = path.split('/')[-1]

    if statement == 'credit':
        df['id_parcel'] = df.apply(lambda row: _generate_foreign_id(
            row['Data de Compra'],
            row['Categoria'],
            row['Descrição'],
            row['Valor (em R$)']
        ), axis=1)
        df['id_classification'] = df.apply(lambda row: _generate_foreign_id(
            row['Categoria'],
            row['Descrição'],
        ), axis=1)

    elif statement == 'debit':
        df['id_classification'] = df.apply(lambda row: _generate_foreign_id(
            row['DESCRIÇÃO']
        ), axis=1)

    return df


class FileManager:
    def __init__(self, root, statement, extension) -> None:
        self.extension = extension
        self.init_folder = f'{root}src/{statement}'
        self.statement = statement

    def list_paths_to_read(self):
        paths = os.listdir(self.init_folder)

        full_paths = []
        for path in paths:

            if not path.split('.')[-1] == 'csv' and self.statement == 'credit':
                continue

            elif not path.split('.')[-1] == 'pdf' and self.statement == 'debit':
                continue

            full_paths.append(os.path.join(f'{self.init_folder}', path))

        return full_paths

    def move_readed_files(self):
        try:
            os.system(f'mkdir {self.init_folder}/readed')
        finally:
            os.system(
                f'mv {self.init_folder}/*.{self.extension} '
                f'{self.init_folder}/readed'
            )

    def delete_temp_file(self):
        os.system('rm temp.pdf')
