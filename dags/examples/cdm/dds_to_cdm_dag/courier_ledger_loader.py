from logging import Logger
from typing import List

from examples.cdm import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class Courier_ledgerObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float
    


class Courier_ledgersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_courier_ledgers(self, courier_ledger_threshold: int, limit: int) -> List[Courier_ledgerObj]:
        with self._db.client().cursor(row_factory=class_row(Courier_ledgerObj)) as cur:
            cur.execute(
                """
                    select * from(SELECT 
                        ROW_NUMBER() OVER (ORDER BY c.courier_id, t."year", t."month") AS id,
                        c.courier_id AS courier_id,
                        c.courier_name,
                        t."year" AS settlement_year,
                        t."month" AS settlement_month,
                        COUNT(DISTINCT o.id) AS orders_count,
                        SUM(f.total_sum) AS orders_total_sum,
                        AVG(d.rate) AS rate_avg,
                        SUM(f.total_sum) * 0.25 AS order_processing_fee,

                        CASE 
                            WHEN AVG(d.rate) < 4 THEN GREATEST(SUM(f.total_sum) * 0.05, 100)
                            WHEN AVG(d.rate) >= 4 AND AVG(d.rate) < 4.5 THEN GREATEST(SUM(f.total_sum) * 0.07, 150)
                            WHEN AVG(d.rate) >= 4.5 AND AVG(d.rate) < 4.9 THEN GREATEST(SUM(f.total_sum) * 0.08, 175)
                            WHEN AVG(d.rate) >= 4.9 THEN GREATEST(SUM(f.total_sum) * 0.10, 200)
                        END AS courier_order_sum,

                        SUM(d.tip_sum)::numeric(14,2) AS courier_tips_sum,

                        (
                            CASE 
                                WHEN AVG(d.rate) < 4 THEN GREATEST(SUM(f.total_sum) * 0.05, 100)
                                WHEN AVG(d.rate) >= 4 AND AVG(d.rate) < 4.5 THEN GREATEST(SUM(f.total_sum) * 0.07, 150)
                                WHEN AVG(d.rate) >= 4.5 AND AVG(d.rate) < 4.9 THEN GREATEST(SUM(f.total_sum) * 0.08, 175)
                                WHEN AVG(d.rate) >= 4.9 THEN GREATEST(SUM(f.total_sum) * 0.10, 200)
                            END + SUM(d.tip_sum)
                        ) * 0.95 AS courier_reward_sum

                    FROM 
                        dds.dm_couriers c 
                    JOIN 
                        dds.dm_deliveries d ON c.id = d.courier_id 
                    JOIN 
                        dds.dm_orders o ON d.order_id = o.id 
                    JOIN 
                        dds.fct_product_sales f ON o.id = f.order_id 
                    JOIN 
                        dds.dm_timestamps t ON o.timestamp_id = t.id  


                    GROUP BY 
                        c.courier_id, c.courier_name, t."year", t."month") dd
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": courier_ledger_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class Courier_ledgerDestRepository:

    def insert_courier_ledger(self, conn: Connection, courier_ledger: Courier_ledgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id,courier_name,settlement_year,settlement_month,orders_count,orders_total_sum,rate_avg,order_processing_fee,courier_order_sum,courier_tips_sum,courier_reward_sum)
                    VALUES (%(courier_id)s,%(courier_name)s,%(settlement_year)s,%(settlement_month)s,%(orders_count)s,%(orders_total_sum)s,%(rate_avg)s,%(order_processing_fee)s,%(courier_order_sum)s,%(courier_tips_sum)s,%(courier_reward_sum)s);
                """,
                {
                    
                    "courier_id": courier_ledger.courier_id,
                    "courier_name": courier_ledger.courier_name,
                    "settlement_year": courier_ledger.settlement_year,
                    "settlement_month": courier_ledger.settlement_month,
                    "orders_count": courier_ledger.orders_count,
                    "orders_total_sum": courier_ledger.orders_total_sum,
                    "rate_avg": courier_ledger.rate_avg,
                    "order_processing_fee": courier_ledger.order_processing_fee,
                    "courier_order_sum": courier_ledger.courier_order_sum,
                    "courier_tips_sum": courier_ledger.courier_tips_sum,
                    "courier_reward_sum": courier_ledger.courier_reward_sum
                },
            )


class Courier_ledgerLoader:
    WF_KEY = "example_courier_ledgers_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = Courier_ledgersOriginRepository(pg_origin)
        self.stg = Courier_ledgerDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_courier_ledgers(self):
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
            load_queue = self.origin.list_courier_ledgers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier_ledger in load_queue:
                self.stg.insert_courier_ledger(conn, courier_ledger)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
