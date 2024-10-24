
from logging import Logger

from examples.stg import  StgEtlSettingsRepository
from examples.stg.api_system_restaurants_dag.pg_saver_restaurants import PgSaverRestaurants
from examples.stg.api_system_restaurants_dag.restaurants_reader import RestaurantsReader
from lib import PgConnect



class RestaurantsLoader:
    _LOG_THRESHOLD = 100
    _SESSION_LIMIT = 10000

    WF_KEY = "example_apisystem_restaurants_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: RestaurantsReader, pg_dest: PgConnect, pg_saver: PgSaverRestaurants, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            

            

            load_queue = self.collection_loader.get_restaurants()
            self.log.info(f"Found {len(load_queue)} documents to sync from restaurants collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["_id"]), d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing restaurants.")

        

            return len(load_queue)
