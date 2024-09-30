from utils.db_conn import PostgreSQL
from langchain_core.prompts import PromptTemplate
from langchain_groq import ChatGroq
from datetime import datetime
import pandas as pd


PROMPT_TEMPLATE = """
    Você é um analista de dados, trabalhando num projeto de limpeza de dados.
    Seu trabalho é escolher uma categoria e sub-categoria adequada para cada
    lançamento financeiro que vou te enviar.

    Todos são transações financeiras de uma pessoas física de uma fatura de cartão de crédito.

    E escolha uma categoria (entre parênteses, dei alguns exemplos):
    - Pagamento (pagamento da fatura do cartão)
    - Saúde (médico, exames, farmácias)
    - Alimentação (supermercados, comida, hortifrutis)
    - Educação (livros, faculdade, cursos)
    - Compras (mercadolivre, shopee, shopping)
    - Transporte (uber, combustível, mecânico)
    - Telefone (recarga de crédito no telefone)
    - Pets (ração, veterinário, petshop)

    Escolha a categoria, baseado na seguinte descrição:
    {text}

    Responda apenas com a categoria.
"""


def classificate(row, now, psql):
    concat_string = ' - '.join(row['categoria_cartao'], row['descricao'])

    prompt = PromptTemplate.from_template(template=PROMPT_TEMPLATE)

    chat = ChatGroq(model="llama3-8b-8192")
    chain = prompt | chat

    response = chain.invoke(concat_string)
    print(f'{concat_string} categorizado como: {response.content}')

    df_classification = {
        'id': 'id_example',
        'categoria_cartao': row['categoria_cartao'],
        'descricao': row['descricao'],
        'categoria_ai': response.content,
        'usage_metadata': response.usage_metadata,
        'model_name': response.response_metadata['model_name'],
        'time': now
    }

    psql.insert_from_pandas(
        schema='clean',
        table_name='classification',
        df=df_classification
    )
    print(f'+{len(df_classification)} linha inserida em clean.classification')


def etl():
    print("Iniciando o job classification")
    psql = PostgreSQL()

    df = psql.query("""
        SELECT DISTINCT
            id_classification as id,
            categoria_cartao,
            descricao
        FROM
            clean.c6_credit
        """)
    print(f'{len(df)} transações distintas retornaram da camada clean')

    psql.create_table("""
        CREATE TABLE IF NOT EXISTS clean.classification (
            id TEXT PRIMARY KEY,
            categoria_cartao TEXT,
            descricao TEXT,
            categoria_ai TEXT,
            usage_metadata JSONB,
            model_name TEXT,
            time TIMESTAMP
        );
    """)

    now = datetime.now()

    df.apply(lambda row: classificate(row, now, psql), axis=1)


if __name__ == "__main__":
    etl()
