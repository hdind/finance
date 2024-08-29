from datetime import datetime
import hashlib

class Metadata():
    def _generate_md5(*args) -> str:
        concat = ''.join(args)
        return hashlib.md5(concat.encode()).hexdigest()

    def add_metadata(self, df, file_path: str):
        df['_time'] = datetime.now()

        df['_md5'] = df.apply(lambda row: self._generate_md5(
            row['Data de Compra'],
            row['Final do Cartão'],
            row['Descrição']
            ), axis=1)

        df['_qt_rows'] = len(df)

        df['_file_name'] = file_path.split('/')[-1]
