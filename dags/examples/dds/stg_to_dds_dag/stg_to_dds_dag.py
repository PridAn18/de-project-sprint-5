import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.stg_to_dds_dag.user_loader import UserLoader
from examples.dds.stg_to_dds_dag.restaurant_loader import RestaurantLoader
from examples.dds.stg_to_dds_dag.timestamp_loader import TimestampLoader
from examples.dds.stg_to_dds_dag.product_loader import ProductLoader
from examples.dds.stg_to_dds_dag.order_loader import OrderLoader
from examples.dds.stg_to_dds_dag.product_sale_loader import ProductSaleLoader
from examples.dds.stg_to_dds_dag.timestamp_loader_deliveries import TimestampLoaderDeliveries
from examples.dds.stg_to_dds_dag.courier_loader import CourierLoader
from examples.dds.stg_to_dds_dag.address_loader import AddressLoader
from examples.dds.stg_to_dds_dag.delivery_loader import DeliveryLoader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_stg_to_dds_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
   

    @task(task_id="load_users")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.
    @task(task_id="load_restaurants")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestaurantLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.
    @task(task_id="load_timestamps")
    def load_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TimestampLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.
    @task(task_id="load_products")
    def load_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products()  # Вызываем функцию, которая перельет данные.
    @task(task_id="load_orders")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OrderLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()  # Вызываем функцию, которая перельет данные.
    @task(task_id="load_productSales")
    def load_productSales():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductSaleLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_productSales()  # Вызываем функцию, которая перельет данные.
    # Инициализируем объявленные таски.
    @task(task_id="load_deliveries_timestamps")
    def load_deliveries_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TimestampLoaderDeliveries(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_deliveries_timestamps()  # Вызываем функцию, которая перельет данные.
    @task(task_id="load_couriers")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CourierLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_couriers()  # Вызываем функцию, которая перельет данные.
    @task(task_id="load_addresses")
    def load_addresses():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = AddressLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_addresses()  # Вызываем функцию, которая перельет данные.
    @task(task_id="load_deliveries")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DeliveryLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    users_dict = load_users()
    restaurants_dict = load_restaurants()
    timestamps_dict = load_timestamps()
    products_dict = load_products()
    orders_dict = load_orders()
    productSales_dict = load_productSales()
    timestampsDeliveries_dict = load_deliveries_timestamps()
    courier_dict = load_couriers()
    address_dict = load_addresses()
    delivery_dict = load_deliveries()
    

    

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    users_dict >> restaurants_dict >> timestamps_dict >> products_dict >> orders_dict >> productSales_dict >> timestampsDeliveries_dict >> courier_dict >> address_dict >> delivery_dict # type: ignore


dds_stg_to_dds_dag = sprint5_example_stg_to_dds_dag()
