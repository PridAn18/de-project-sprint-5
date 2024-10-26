from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrderObj(BaseModel):
    id: int
    user_id: int
    restaurant_id: int
    timestamp_id: int
    order_key: str
    order_status: str
    


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
    #{"_id": "66e521c423422649d3b746b0", "bonus_grant": 644, "bonus_payment": 346, "cost": 13209, "date": "2024-09-14 05:40:20", "final_status": "CLOSED", "order_items": [{"id": "800d4b1283e01fb243124225", "name": "Бриани с овощами", "price": 499, "quantity": 1}, {"id": "3f7848ea94b146dedf030c2a", "name": "Дал Макхни", "price": 499, "quantity": 5}, {"id": "95f74625be0428e4b2380310", "name": "Проун Пакора", "price": 790, "quantity": 2}, {"id": "9df848fe96dd0b097380d456", "name": "Бриани с креветками", "price": 690, "quantity": 5}, {"id": "2c0b49dd92d3e411f8f5fabd", "name": "Сладкий Ласси", "price": 150, "quantity": 4}, {"id": "36d54f529ef3ac5e05210178", "name": "Баттер Чикке", "price": 599, "quantity": 5}, {"id": "e86d440f87ba45c8280310dc", "name": "Чили Проун", "price": 790, "quantity": 1}, {"id": "35dd4493974018de1ed0e036", "name": "Рис с шафраном", "price": 200, "quantity": 4}], "payment": 13209, "restaurant": {"id": "ef8c42c19b7518a9aebec106"}, "statuses": [{"dttm": "2024-09-14 05:40:20", "status": "CLOSED"}, {"dttm": "2024-09-14 05:21:31", "status": "DELIVERING"}, {"dttm": "2024-09-14 04:24:38", "status": "COOKING"}, {"dttm": "2024-09-14 03:41:19", "status": "OPEN"}], "update_ts": "2024-09-14 05:40:20", "user": {"id": "626a81ce9a8cd1920641e2b8"}}
    def list_orders(self, order_threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    select * from (select ROW_NUMBER() OVER (ORDER BY (SELECT 1))::int4 AS id, du.id as user_id, dr.id as restaurant_id, dt.id as timestamp_id, oo.order_key as order_key,oo.final_status as order_status
                    from (select object_value::JSON->>'_id' AS order_key,object_value::JSON->>'final_status' AS final_status, object_value::JSON->>'date' AS date, object_value::JSON->'user'->>'id' as user, object_value::JSON->'restaurant'->>'id' AS restaurant
                    from stg.ordersystem_orders) oo inner join dds.dm_timestamps dt on oo.date = dt.ts::text inner join dds.dm_users du on oo.user = du.user_id inner join dds.dm_restaurants dr on oo.restaurant = dr.restaurant_id ) aa
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrderDestRepository:

    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id,restaurant_id,timestamp_id,order_key,order_status)
                    VALUES (%(user_id)s,%(restaurant_id)s,%(timestamp_id)s,%(order_key)s,%(order_status)s);
                """,
                {
                    
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "order_key": order.order_key,
                    "order_status": order.order_status
                    
                },
            )


class OrderLoader:
    WF_KEY = "example_orders_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 3050  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_origin)
        self.stg = OrderDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.stg.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
