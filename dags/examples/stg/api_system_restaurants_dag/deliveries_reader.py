from datetime import datetime
from typing import Dict, List
import logging
import requests



log = logging.getLogger(__name__)
NICKNAME = 'a.navaro'
COHORT_NUMBER = '29'
API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f' # можно сделать 
SORT_DIRECTION = 'asc'  
OFFSET = 0
HEADERS = {
        'X-Nickname': NICKNAME,
        'X-Cohort': COHORT_NUMBER,
        'X-API-KEY': API_KEY
}

class DeliveriesReader:
    

    def get_deliveries(self, load_threshold: datetime, limit) -> List[Dict]:
        # Формируем фильтр: больше чем дата последней загрузки
        filter = load_threshold.strftime("%Y-%m-%d %H:%M:%S")
        
        # Формируем сортировку по update_ts. Сортировка обязательна при инкрементальной загрузке.
        
        # Получаем текущую дату и время
        now = datetime.now()

        # Формируем строку, представляющую дату и время в нужном формате
        TO =  now.strftime("%Y-%m-%d %H:%M:%S")
        url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?restaurant_id=&from={filter}&to={TO}&sort_field=order_ts&sort_direction={SORT_DIRECTION}&limit={limit}&offset={OFFSET}'
    
    
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            deliveries = response.json()
        # Вычитываем документы из MongoDB с применением фильтра и сортировки.
        docs = list(deliveries)
        return docs
