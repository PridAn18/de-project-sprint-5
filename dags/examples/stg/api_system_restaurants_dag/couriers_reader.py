import requests

from typing import Dict, List



NICKNAME = 'a.navaro'
COHORT_NUMBER = '29'
API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'
SORT_FIELD = 'id'  # или 'name'
SORT_DIRECTION = 'asc'  # или 'desc'
LIMIT = 50
OFFSET = 0

HEADERS = {
        'X-Nickname': NICKNAME,
        'X-Cohort': COHORT_NUMBER,
        'X-API-KEY': API_KEY
}

class CouriersReader:
    

    def get_couriers(self) -> List[Dict]:
        # Формируем фильтр: больше чем дата последней загрузки
        url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={SORT_FIELD}&sort_direction={SORT_DIRECTION}&limit={LIMIT}&offset={OFFSET}'
    
    
        response = requests.get(url, headers=HEADERS)
    
        if response.status_code == 200:
            couriers = response.json()
        # Вычитываем документы из MongoDB с применением фильтра и сортировки.
        docs = list(couriers)
        return docs
