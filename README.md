# Проект 5-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 5-го спринта

Проектная работа по DWH для нескольких источников: MongoDB, API, PostegreSQL

'init_schema_dag' ДАГ на создание слоёв данных.

В рамках работы со Staging слоем:
* Выгржаются данные о ресторанах, курьерах, доставщиках из MongoDB, также выгружаются данные из подсистемы бонусов об использовании бонусов по API, выгружаются данные из системы заказов PostgreSQL. Реализована инкрементальная загрука, создав таблицу с последним выгруженным id, с контролем размера BATCHа.

В рамках работы с DDS слоем:
* Читаются данные со Staging. Реализована инкрементальная загрука, создав таблицу с последним выгруженным id, с контролем размера BATCHа.

В рамках работы с CDM слоем:
* Читаются данные с DDS. Реализована инкрементальная загрука, создав таблицу с последним выгруженным id, с контролем размера BATCHа.

### Структура репозитория
1. Папка `migrations` хранит файлы миграции.
2. В папке `dags` хранятся файлы для работы DAGов Airflow: 
    * В папке `examples` хранятся файлы для работы DAGов Airflow:
		* В папке `stg` 'init_schema_dag' ДАГ на создание слоёв данных и 3 ДАГа с загрузкой данных из 3 источников в STG слой.
			* 'bonus_system_ranks_dag' подгружает данные из системы бонусов
			* 'api_system_restaurants_dag' подгружает данные из API системы
			* 'order_system_restaurants_dag' подгружает данные из системы заказов
		* В папке `dds` хранятся файлы для работы DAG с загрузкой в dds.
		* В папке `cdm` хранятся файлы для работы DAG с загрузкой в cdm.

### Как запустить контейнер
Запустите локально команду:

```
docker run \
-d \
-p 3000:3000 \
-p 3002:3002 \
-p 15432:5432 \
cr.yandex/crp1r8pht0n0gl25aug1/de-pg-cr-af:latest
```

После того как запустится контейнер, вам будут доступны:
- Airflow
	- `localhost:3000/airflow`
- БД
	- `jovyan:jovyan@localhost:15432/de`
