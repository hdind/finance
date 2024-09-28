from configs.config import PSQL_CONN
from sqlalchemy import create_engine
import psycopg2


class PostgreSQL:
    def __init__(self) -> None:
        self.host = "localhost"
        self.port = "5433"
        self.database = PSQL_CONN["psql_db"]
        self.user = PSQL_CONN["psql_username"]
        self.password = PSQL_CONN["psql_password"]

    def __connect(self):
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        except Exception as error:
            print(f"Erro ao conectar ao banco de dados: {error}")

        return conn

    def query(self, query):
        conn = self.__connect()
        cursor = conn.cursor()

        cursor.execute(query)
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        return result

    def create_table(self, query):
        conn = self.__connect()
        cursor = conn.cursor()

        cursor.execute(query)

        conn.commit()
        print('Tabela criada com sucesso!')

    def insert_from_pandas(self, schema, table_name, df):
        engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:"
            f"{self.port}/{self.database}"
        )
        try:
            df.to_sql(
                f"{table_name}",
                con=engine,
                if_exists="append",
                index=False,
                schema='raw'   
            )
            print(f"+{len(df)} linhas inseridas na {schema}.{table_name}")

        except Exception as e:
            raise print(f"Ocorreu um erro: {e}")
