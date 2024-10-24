from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class TimestampDeliveryObj(BaseModel):
    id: int
    ts: str
    year: str
    month: str
    day: str
    time: str
    date: str
    


class TimestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, timestamp_threshold: int, limit: int) -> List[TimestampDeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampDeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT id, TO_CHAR(TO_TIMESTAMP(object_value::JSON->>'delivery_ts', 'YYYY-MM-DD"T"HH24:MI:SS.000'), 'YYYY-MM-DD" "HH24:MI:SS') AS ts, DATE_PART('year', (object_value::JSON->>'delivery_ts')::TIMESTAMP)::int2 AS year, DATE_PART('month', (object_value::JSON->>'delivery_ts')::TIMESTAMP)::int2 AS month,  DATE_PART('day', (object_value::JSON->>'delivery_ts')::TIMESTAMP)::int2 AS day, TO_CHAR(TO_TIMESTAMP(object_value::JSON->>'delivery_ts', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), 'YYYY-MM-DD') AS date, TO_CHAR(TO_TIMESTAMP(object_value::JSON->>'delivery_ts', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), 'HH24:MI:SS') AS time
                    FROM stg.apisystem_deliveries
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": timestamp_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class TimestampDestRepository:

    def insert_timestampDelivery(self, conn: Connection, timestamp: TimestampDeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts,year,month,day,time,date)
                    VALUES (%(ts)s,%(year)s,%(month)s,%(day)s,CAST(%(time)s AS time),CAST(%(date)s AS date));
                """,
                {
                    
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "time": timestamp.time,
                    "date": timestamp.date
                    
                },
            )


class TimestampLoaderDeliveries:
    WF_KEY = "example_deliveries_timestamps_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 20000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TimestampsOriginRepository(pg_origin)
        self.stg = TimestampDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries_timestamps(self):
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
            load_queue = self.origin.list_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for timestamp in load_queue:
                self.stg.insert_timestampDelivery(conn, timestamp)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
