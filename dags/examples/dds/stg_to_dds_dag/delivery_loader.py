from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DeliveryObj(BaseModel):
    id: int
    delivery_id: str
    courier_id: int
    order_id: int
    timestamp_id: int
    address_id: int
    rate: int
    tip_sum: int
    
    


class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
    #{"_id": "66e521c423422649d3b746b0", "bonus_grant": 644, "bonus_payment": 346, "cost": 13209, "date": "2024-09-14 05:40:20", "final_status": "CLOSED", "order_items": [{"id": "800d4b1283e01fb243124225", "name": "Бриани с овощами", "price": 499, "quantity": 1}, {"id": "3f7848ea94b146dedf030c2a", "name": "Дал Макхни", "price": 499, "quantity": 5}, {"id": "95f74625be0428e4b2380310", "name": "Проун Пакора", "price": 790, "quantity": 2}, {"id": "9df848fe96dd0b097380d456", "name": "Бриани с креветками", "price": 690, "quantity": 5}, {"id": "2c0b49dd92d3e411f8f5fabd", "name": "Сладкий Ласси", "price": 150, "quantity": 4}, {"id": "36d54f529ef3ac5e05210178", "name": "Баттер Чикке", "price": 599, "quantity": 5}, {"id": "e86d440f87ba45c8280310dc", "name": "Чили Проун", "price": 790, "quantity": 1}, {"id": "35dd4493974018de1ed0e036", "name": "Рис с шафраном", "price": 200, "quantity": 4}], "payment": 13209, "restaurant": {"id": "ef8c42c19b7518a9aebec106"}, "statuses": [{"dttm": "2024-09-14 05:40:20", "status": "CLOSED"}, {"dttm": "2024-09-14 05:21:31", "status": "DELIVERING"}, {"dttm": "2024-09-14 04:24:38", "status": "COOKING"}, {"dttm": "2024-09-14 03:41:19", "status": "OPEN"}], "update_ts": "2024-09-14 05:40:20", "user": {"id": "626a81ce9a8cd1920641e2b8"}}
    def list_deliveries(self, delivery_threshold: int, limit: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    select * from (
                    select ROW_NUMBER() OVER (ORDER BY (SELECT 1))::int4 AS id, oo.delivery_id, du.id as courier_id, dr.id as order_id, dt.id as timestamp_id, da.id as address_id ,oo.rate as rate,oo.tip_sum as tip_sum
                    from (select object_value::JSON->>'delivery_id' as delivery_id, TO_CHAR(TO_TIMESTAMP(object_value::JSON->>'delivery_ts', 'YYYY-MM-DD"T"HH24:MI:SS'), 'YYYY-MM-DD" "HH24:MI:SS') AS delivery_ts, object_value::JSON->>'courier_id' as courier_id, object_value::JSON->>'order_id' AS order_id, object_value::JSON->>'address' as address_id, object_value::JSON->>'rate' as rate,object_value::JSON->>'tip_sum' as tip_sum
                    from stg.apisystem_deliveries) oo inner join dds.dm_timestamps dt on oo.delivery_ts = dt.ts::text inner join dds.dm_couriers du on oo.courier_id = du.courier_id inner join dds.dm_orders dr on oo.order_id = dr.order_key inner join dds.dm_addresses da on oo.address_id = da.address
                    ) aa
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id,courier_id,order_id,timestamp_id,address_id,rate,tip_sum)
                    VALUES (%(delivery_id)s,%(courier_id)s,%(order_id)s,%(timestamp_id)s,%(address_id)s,%(rate)s,%(tip_sum)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        order_id = EXCLUDED.order_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        address_id = EXCLUDED.address_id,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    
                    "delivery_id": delivery.delivery_id,
                    "courier_id": delivery.courier_id,
                    "order_id": delivery.order_id,
                    "timestamp_id": delivery.timestamp_id,
                    "address_id": delivery.address_id,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum
                                    
                    
                },
            )


class DeliveryLoader:
    WF_KEY = "example_deliveries_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 3050  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginRepository(pg_origin)
        self.stg = DeliveryDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
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
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.stg.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
