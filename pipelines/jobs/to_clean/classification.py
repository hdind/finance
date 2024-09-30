from utils.db_conn import PostgreSQL
from langchain_core.prompts import PromptTemplate
from langchain_groq import ChatGroq
import pandas as pd


PROMPT_TEMPLATE = """
    Você é um analista de dados, trabalhando num projeto de limpeza de dados.
    Seu trabalho é escolher uma categoria e sub-categoria adequada para cada 
    lançamento financeiro que vou te enviar.

    Todos são transações financeiras de uma pessoas física de uma fatura de cartão de crédito.

    E escolha uma categoria entre:
    - Pagamento
    - Saúde
    - Mercado
    - Educação
    - Compras
    - Transporte
    - Telefone

    Escolha a categoria, baseado na seguinte descrição:
    {text}

    Responda apenas com a categoria.
"""

def classificate(*args):
    concat_string = ' - '.join(args)
    prompt = PromptTemplate.from_template(template=PROMPT_TEMPLATE)

    chat = ChatGroq(model="llama3-8b-8192")
    chain = prompt | chat

    response = chain.invoke(concat_string)

    
