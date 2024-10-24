from airflow import DAG
from airflow.operators.python import PythonOperator

import logging
from datetime import datetime
from examples.stg.api_system_restaurants_dag.pg_saver_restaurants import PgSaverRestaurants
from examples.stg.api_system_restaurants_dag.restaurants_reader import RestaurantsReader
from examples.stg.api_system_restaurants_dag.restaurants_loader import RestaurantsLoader
from examples.stg.api_system_restaurants_dag.pg_saver_couriers import PgSaverCouriers
from examples.stg.api_system_restaurants_dag.couriers_reader import CouriersReader
from examples.stg.api_system_restaurants_dag.couriers_loader import CouriersLoader
from examples.stg.api_system_restaurants_dag.pg_saver_deliveries import PgSaverDeliveries
from examples.stg.api_system_restaurants_dag.deliveries_reader import DeliveriesReader
from examples.stg.api_system_restaurants_dag.deliveries_loader import DeliveriesLoader
from lib import ConnectionBuilder



# Определите ваши параметры
NICKNAME = 'a.navaro'
COHORT_NUMBER = '29'
API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'
SORT_FIELD = 'id'  # или 'name'
SORT_DIRECTION = 'asc'  # или 'desc'
LIMIT = 50
OFFSET = 0
RESTAURANT_ID = 0
FROM = '2022-01-01 00:00:00'

HEADERS = {
        'X-Nickname': NICKNAME,
        'X-Cohort': COHORT_NUMBER,
        'X-API-KEY': API_KEY
}
dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
log = logging.getLogger(__name__)

def get_restaurants():
    #dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    pg_saver_restaurants = PgSaverRestaurants()
    collection_reader = RestaurantsReader()
    loader = RestaurantsLoader(collection_reader, dwh_pg_connect, pg_saver_restaurants, log)
    loader.run_copy()
    
    
    
    
def get_couriers():
    pg_saver_couriers = PgSaverCouriers()
    collection_reader = CouriersReader()
    loader = CouriersLoader(collection_reader, dwh_pg_connect, pg_saver_couriers, log)
    loader.run_copy()
    
def get_deliveries():
    pg_saver_deliveries = PgSaverDeliveries()
    collection_reader = DeliveriesReader()
    loader = DeliveriesLoader(collection_reader, dwh_pg_connect, pg_saver_deliveries, log)
    loader.run_copy()
          


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
}

with DAG('get_data_api_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    get_restaurants_task = PythonOperator(
        task_id='get_restaurants',
        python_callable=get_restaurants
    )
    get_couriers_task = PythonOperator(
        task_id='get_couriers',
        python_callable=get_couriers
    )
    get_deliveries_task = PythonOperator(
        task_id='get_deliveries',
        python_callable=get_deliveries
    )

get_restaurants_task >> get_couriers_task >> get_deliveries_task