import logging

import pendulum
from airflow.decorators import dag, task

from examples.cdm.dds_to_cdm_dag.settlement_report_loader import Settlement_reportLoader
from examples.cdm.dds_to_cdm_dag.courier_ledger_loader import Courier_ledgerLoader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_dds_to_cdm_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
   

    
    @task(task_id="load_settlement_reports")
    def load_settlement_reports():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = Settlement_reportLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_settlement_reports()  # Вызываем функцию, которая перельет данные.
    @task(task_id="load_courier_ledgers")
    def load_courier_ledgers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = Courier_ledgerLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_courier_ledgers()  # Вызываем функцию, которая перельет данные.

    
    settlement_reports_dict = load_settlement_reports()
    courier_ledgers_dict = load_courier_ledgers()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    settlement_reports_dict >> courier_ledgers_dict # type: ignore


dds_to_cdm_dag = sprint5_example_dds_to_cdm_dag()
