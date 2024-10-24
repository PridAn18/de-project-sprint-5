
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaverCouriers:

    def save_object(self, conn: Connection, id: str, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.apisystem_couriers(object_id, object_value)
                    VALUES (%(id)s, %(val)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value;
                """,
                {
                    "id": id,
                    "val": str_val
                    
                }
            )
