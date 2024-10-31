from configs.config import PSQL_CONN
from sqlalchemy import create_engine
import pandas as pd
import psycopg2


class PostgreSQL:
    def __init__(self) -> None:
        self.host = "localhost"
        self.port = "5433"
        self.database = PSQL_CONN["psql_db"]
        self.user = PSQL_CONN["psql_username"]
        self.password = PSQL_CONN["psql_password"]

        self.psycopg2_conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

        self.sqlalchemy_conn = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:"
            f"{self.port}/{self.database}"
        )

    def query(self, query):
        return pd.read_sql(query, self.sqlalchemy_conn)

    def create_table(self, query):
        cursor = self.psycopg2_conn.cursor()

        cursor.execute(query)

        self.psycopg2_conn.commit()

        cursor.close()
        self.psycopg2_conn.close()
        print('Tabela criada com sucesso!')

    def insert_from_pandas(self, schema, table_name, df):
        df.to_sql(
            f"{table_name}",
            con=self.sqlalchemy_conn,
            if_exists="append",
            index=False,
            schema=schema
        )
        print(f"+{len(df)} linhas inseridas na {schema}.{table_name}")
