from logging import Logger
from typing import List

from examples.cdm import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class Settlement_reportObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    settlement_date: str
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float
    


class Settlement_reportsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_settlement_reports(self, settlement_report_threshold: int, limit: int) -> List[Settlement_reportObj]:
        with open('settlement_report.sql', 'r') as file:
            sql_query = file.read()
        with self._db.client().cursor(row_factory=class_row(Settlement_reportObj)) as cur:
            cur.execute(
                f"""
                    select * from(
                    {sql_query}) dd
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": settlement_report_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class Settlement_reportDestRepository:

    def insert_settlement_report(self, conn: Connection, settlement_report: Settlement_reportObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report(restaurant_id,restaurant_name,settlement_date,orders_count,orders_total_sum,orders_bonus_payment_sum,orders_bonus_granted_sum,order_processing_fee,restaurant_reward_sum)
                    VALUES (%(restaurant_id)s,%(restaurant_name)s,%(settlement_date)s,%(orders_count)s,%(orders_total_sum)s,%(orders_bonus_payment_sum)s,%(orders_bonus_granted_sum)s,%(order_processing_fee)s,%(restaurant_reward_sum)s)
                    ON CONFLICT (restaurant_id,settlement_date) DO UPDATE
                    SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    
                    "restaurant_id": settlement_report.restaurant_id,
                    "restaurant_name": settlement_report.restaurant_name,
                    "settlement_date": settlement_report.settlement_date,
                    "orders_count": settlement_report.orders_count,
                    "orders_total_sum": settlement_report.orders_total_sum,
                    "orders_bonus_payment_sum": settlement_report.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": settlement_report.orders_bonus_granted_sum,
                    "order_processing_fee": settlement_report.order_processing_fee,
                    "restaurant_reward_sum": settlement_report.restaurant_reward_sum
                    
                },
            )


class Settlement_reportLoader:
    WF_KEY = "example_settlement_reports_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = Settlement_reportsOriginRepository(pg_origin)
        self.stg = Settlement_reportDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_settlement_reports(self):
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
            load_queue = self.origin.list_settlement_reports(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for settlement_report in load_queue:
                self.stg.insert_settlement_report(conn, settlement_report)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
