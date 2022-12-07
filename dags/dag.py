from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from airflow.models import Variable
import requests
import psycopg2

def configs():
    dag_config = Variable.get("variables_config", deserialize_json=True)
    configs=[dag_config["AIRFLOW_API_KEY"], dag_config["POSTGRES_USER"],
             dag_config["POSTGRES_PASSWORD"], dag_config["POSTGRES_DB"]]
    print('var', configs)
    return configs

TOKEN = str(configs()[0])
print(TOKEN)
base = 'USD'
date = '2022-01-01'
tickers = ['USD', 'EUR', 'RUB']

def get_currencies_task(ticker_from, ticker_to, ti):
    obj = Currency()
    value = obj.pair(ticker_from, ticker_to)#....#
    print(f"курс одного {ticker_from} к {ticker_to}: {value}")
    date = datetime.today().strftime('%Y-%m-%d')
    tuple_to_insert = (date, ticker_from, ticker_to, value)
    ti.xcom_push(key = 'value', value = tuple_to_insert)

def insert_data(ti):
    values_to_insert = ti.xcom_pull(key='value', task_ids=task_list)
    values_to_insert = [tuple(value) for value in values_to_insert]
    query = """INSERT INTO pairs(date, ticker_from, ticker_to, amount) VALUES(%s,%s,%s,%s)"""
    conn = None
    try:
        conn = psycopg2.connect(host ='database', user= str(configs()[1]), password= str(configs()[2]), dbname= str(configs()[3]))
        print('Postgresql connection created!')
        conn.autocommit = True
        cur = conn.cursor()
        cur.executemany(query, values_to_insert)
        print('Data successfully inserted!')
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def materialized_view_creation():
    query = """CREATE MATERIALIZED VIEW IF NOT EXISTS predictions AS(
                SELECT date,ticker_from, ticker_to, AVG(amount) 
                OVER(PARTITION BY ticker_from, ticker_to ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
                AS amount_prediction
                FROM pairs
                ORDER BY date DESC)"""
    conn = None
    try:
        conn = psycopg2.connect(host ='database', user= str(configs()[1]), password= str(configs()[2]), dbname= str(configs()[3]))
        print('Postgresql connection created!')
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(query)
        print('Data successfully inserted!')
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
           
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
    
#DAG
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
        
#TASKS
    encryption = PythonOperator(
	    task_id ='encryption',
        python_callable = configs)
    
    tickers_to_use = tickers
    task_list = [] 
    for i, ticker_from in enumerate(tickers_to_use):
        for j, ticker_to in enumerate(tickers_to_use[i+1:]):
            print(ticker_from, ticker_to)
            ti = PythonOperator(
                task_id = f'task_{i}_{j}',
                python_callable = get_currencies_task,
                op_args = (ticker_from, ticker_to)
            )
            task_list.append(f'task_{i}_{j}')
    
    create_table = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id  = 'postgres_localhost',
        sql = """
        CREATE TABLE IF NOT EXISTS pairs(
            date TIMESTAMP NOT NULL,
            ticker_from VARCHAR(3) NOT NULL,
            ticker_to VARCHAR(3) NOT NULL,
            amount FLOAT NOT NULL,
        PRIMARY KEY (date, ticker_from, ticker_to));
        """,)

    fill_db = PythonOperator(
	    task_id ='insert_data',
        python_callable = insert_data)

    materialized_view = PythonOperator(
	task_id ='materialized_view',
    python_callable = materialized_view_creation)

    refresh_materialized = PostgresOperator(
        task_id = "refresh_materialized",
        postgres_conn_id  = 'postgres_localhost',
        sql = """REFRESH MATERIALIZED VIEW predictions
        """,)

    create_prediction_table = PostgresOperator(
        task_id = "create_prediction_table",
        postgres_conn_id  = 'postgres_localhost',
        sql = """
        CREATE TABLE IF NOT EXISTS prediction_table(
            date TIMESTAMP NOT NULL,
            ticker_from VARCHAR(3) NOT NULL,
            ticker_to VARCHAR(3) NOT NULL,
            amount FLOAT NOT NULL,
        PRIMARY KEY (date, ticker_from, ticker_to));
        """,)
    
    procedure_insert = PostgresOperator(
        task_id = "procedure_insert",
        postgres_conn_id  = 'postgres_localhost',
        sql = """
            CREATE OR REPLACE PROCEDURE insert_data()
            LANGUAGE SQL
            AS $$
            INSERT INTO prediction_table
            SELECT date + interval '1 day', ticker_from, ticker_to, amount_prediction from predictions
            WHERE date = CURRENT_DATE 
            $$;
            CALL insert_data(); """,)  
     
    metric_mse = PostgresOperator(
        task_id = "metric_mse",
        postgres_conn_id  = 'postgres_localhost',
          sql =  """SELECT pairs.date, pairs.ticker_from, pairs.ticker_to, amount, amount_prediction, 
                    AVG(POWER(amount - amount_prediction, 2)) 
                    OVER (PARTITION BY pairs.ticker_from, pairs.ticker_to 
                        ORDER BY pairs.date DESC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
                        AS metric_mse 
                    FROM pairs
                    JOIN (
                        SELECT date, ticker_from, ticker_to, amount_prediction
                        FROM predictions
                    ) AS preds
                    ON pairs.date = preds.date
                        AND pairs.ticker_from = preds.ticker_from
                        AND pairs.ticker_to = preds.ticker_to""",) 
    
    encryption >> ti >> create_table >> fill_db >> materialized_view >> refresh_materialized >> create_prediction_table>>procedure_insert >> metric_mse
