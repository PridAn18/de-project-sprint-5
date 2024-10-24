from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaverDeliveries:

    def save_object(self, conn: Connection, id: str, order_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.apisystem_deliveries(object_id, object_value, order_ts)
                    VALUES (%(id)s, %(val)s, %(order_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        order_ts = EXCLUDED.order_ts;
                """,
                {
                    "id": id,
                    "val": str_val,
                    "order_ts": order_ts
                }
            )
