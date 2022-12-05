from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

import requests

TOKEN = '82adcbbbae7a13d16e0b68fa1f50c023'
base = 'USD'
date = '2022-01-01'
tickers = ['USD', 'EUR', 'RUB']

def get_c(ticker_from, ticker_to):
    obj = Currency()
    value = obj.pair(ticker_from, ticker_to)#....#
    print(f"курс одного {ticker_from} к {ticker_to}: {value}")


class Currency:
    def get_currencies(self):
        x = requests.get('https://api.currencybeacon.com/v1/currencies', params={'api_key': TOKEN})
        response = x.json()
        tickers_to_use = list(response['response']['fiats'].keys())
        return tickers_to_use
    
    def conversion_all_to_all(self):
        ticker = Currency()
        tickers_to_use = ticker.get_currencies()
        for i, ticker_from in enumerate(tickers_to_use):
            for ticker_to in tickers_to_use[i+1:]:
                response =  requests.get('https://api.currencybeacon.com/v1/convert', params={'api_key': TOKEN, 'from': ticker_from, 'to': ticker_to, 'amount': 1})
                print(f"курс одного {ticker_from} к {ticker_to}: {response.json()['response']['value']}")
    
    def pair(self, ticker_from, ticker_to):
        response =  requests.get('https://api.currencybeacon.com/v1/convert', params={'api_key': TOKEN, 'from': ticker_from, 'to': ticker_to, 'amount': 1})
        return response.json()['response']['value']
        
    
    def convert_USD_EUR_RUB(self, tickers):
        tickers_to_use = tickers
        for i, ticker_from in enumerate(tickers_to_use):
            for ticker_to in tickers_to_use[i+1:]:
                response =  requests.get('https://api.currencybeacon.com/v1/convert', params={'api_key': TOKEN, 'from': ticker_from, 'to': ticker_to, 'amount': 1})
                print(f"курс одного {ticker_from} к {ticker_to}: {response.json()['response']['value']}")
    
    def latest(self, base):
        x = requests.get('https://api.currencybeacon.com/v1/latest', params={'api_key': TOKEN, 'base': base})
        response = x.json()
        return response
    
    def historical(self, base, date):
        x = requests.get('https://api.currencybeacon.com/v1/historical', params={'api_key': TOKEN, 'base': base, 'date': date})
        response = x.json()
        return response

args = {
    'owner': 'lofitravel',
    'start_date':datetime(2022, 12, 4),
    'provide_context':True
}

with DAG(
    'dag',
    description='my_dag',
    schedule_interval='@daily', #'0 0 * * *'
    catchup=False,
    default_args=args) as dag:

    tickers_to_use = tickers   
    for i, ticker_from in enumerate(tickers_to_use):
        for j, ticker_to in enumerate(tickers_to_use[i+1:]):
            print(ticker_from, ticker_to)
            ti = PythonOperator(
                task_id = f'task_{i}_{j}',
                python_callable = get_c,
                op_args = (ticker_from, ticker_to)
            )

    create_table = PostgresOperator(
        task_id = "create_table",
        # conn_id = "id",
        postgres_conn_id = 'postgres_localhost',
        sql = """
        CREATE TABLE IF NOT EXISTS pairs(
            date TIMESTAMP NOT NULL,
            ticker_from VARCHAR(3) NOT NULL,
            ticker_to VARCHAR(3) NOT NULL,
            amount FLOAT NOT NULL);
        """,
    )