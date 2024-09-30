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
    - Fatura (pagamento da fatura do cartão)
    - Saúde (médico, exames, farmácias)
    - Alimentação (supermercados, comida, hortifrutis)
    - Educação (livros, faculdade, cursos)
    - Compras (mercadolivre, shopee, shopping)
    - Transporte (uber, combustível, mecânico)
    - Telefone (recarga de crédito no telefone)
    - Pets (ração, veterinário, petshop)
    - Moradia

    Escolha a categoria, baseado na seguinte descrição:
    {text}

    Responda apenas com a categoria.
"""


def clean_response_content(content):
    categories = [
        'Pagamento',
        'Saúde',
        'Alimentação',
        'Educação',
        'Compras',
        'Transporte',
        'Telefone',
        'Pets',
        'Moradia'
    ]

    content = content.replace('(', '').replace(')', '').split(' ')[0]
    return None if content not in categories else content


def classificate(row, now, psql):
    concat_string = ' - '.join([row['categoria_cartao'], row['descricao']])

    prompt = PromptTemplate.from_template(template=PROMPT_TEMPLATE)

    chat = ChatGroq(model="llama3-8b-8192")
    chain = prompt | chat

    response = chain.invoke(concat_string)
    content = clean_response_content(response.content)

    if content:
        print(f'{concat_string} categorizado como: {response.content}')

        df_classification = pd.DataFrame([{
            'id': row['id'],
            'categoria_cartao': row['categoria_cartao'],
            'descricao': row['descricao'],
            'categoria_ai': content,
            'input_tokens': response.usage_metadata['input_tokens'],
            'output_tokens': response.usage_metadata['output_tokens'],
            'total_tokens': response.usage_metadata['total_tokens'],
            'model_name': response.response_metadata['model_name'],
            'time': now
        }])

        psql.insert_from_pandas(
            schema='clean',
            table_name='classification',
            df=df_classification
        )
    else:
        print(f'{response.content} não foi classificado corretamente')


def etl():
    print("Iniciando o job classification")
    psql = PostgreSQL()

    df = psql.query("""
        WITH to_classification as (
            SELECT
                DISTINCT id_classification,
                categoria_cartao,
                descricao
            FROM
                clean.c6_credit
            UNION ALL
            SELECT
                DISTINCT id_classification,
                '' as categoria_cartao,
                descricao
            FROM
                clean.c6_debit
        )
        SELECT
            a.id_classification AS id,
            a.categoria_cartao,
            a.descricao
        FROM
            to_classification a
        LEFT JOIN clean.classification c ON
            a.id_classification = c.id
        WHERE
            c.categoria_ai IS NULL;
        """)
    print(f'{len(df)} transações distintas retornaram da camada clean')

    psql.create_table("""
        --DROP TABLE clean.classification;
        CREATE TABLE IF NOT EXISTS clean.classification (
            id TEXT PRIMARY KEY,
            categoria_cartao TEXT,
            descricao TEXT,
            categoria_ai TEXT,
            input_tokens TEXT,
            output_tokens TEXT,
            total_tokens TEXT,
            model_name TEXT,
            time TIMESTAMP
        );
    """)

    now = datetime.now()

    df.apply(lambda row: classificate(row, now, psql), axis=1)


if __name__ == "__main__":
    etl()
