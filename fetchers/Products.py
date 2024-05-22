import requests
import json
from typing import Dict, AnyStr, List, Tuple
from copy import deepcopy
from ..tools.functions import flatten_dict


class Products:
    """
    Класс для получения актуальной информации о товарах магазина, а именно:
        - Полный список активных и неактивных товаров.
        - Информациях о категориях.
        - Информациях о комиссиях.
        - Информациях об остатках по моделям FBO, FBS и др.
        - Статус участия в акциях.
        - И другое.
    """

    def __init__(self, headers: Dict) -> None:
        """
        Используемые api-методы:
            - v2/product/list.
            - v4/product/info/prices.
            - v3/product/info/stocks.
            - v2/product/info/list.
        Ссылка на документацию к методам:
            - https://docs.ozon.ru/api/seller/#operation/ProductAPI_GetProductList.
            - https://docs.ozon.ru/api/seller/#operation/ProductAPI_GetProductInfoPricesV4.
            - https://docs.ozon.ru/api/seller/#operation/ProductAPI_GetProductInfoStocksV3.
            - https://docs.ozon.ru/api/seller/#operation/ProductAPI_GetProductInfoListV2.
        Полученные данные хранятся в следующих атрибутах:
            - self.products.
            - self.prices.
            - self.stocks.
            - self.product_info.
        Для получения объединенного объекта используйте метод: self.full_data.

        Алгоритм работы:
            - В цикле происходит сбор информации о товарах,
              начиная с v2/product/list, который передает информацию об ID товаров,
              что необходимо для последующих api-запросов.

        :param headers: Параметры для входа в личный кабинет: {"Client-Id": "string", "Api-Key": "string"}.
        """
        self.headers = headers
        self.urls = {"product_list": "https://api-seller.ozon.ru/v2/product/list",
                     "prices_info": "https://api-seller.ozon.ru/v4/product/info/prices",
                     "stocks_info": "https://api-seller.ozon.ru/v3/product/info/stocks",
                     "product_info_list": "https://api-seller.ozon.ru/v2/product/info/list"}

        self.products = []
        self.prices = []
        self.stocks = []
        self.product_info = []

        self.body = {
            "filter": {
                "offer_id": [],
                "product_id": [],
                "visibility": "ALL"
            },
            "last_id": None,
            "limit": 1000
        }

    def _post_request(self, api_method: AnyStr) -> Tuple:
        """
        Функция отправляет запрос к серверу Ozon.

        :param api_method: API-метод, который будет использован для создания post-запроса.
        """
        try:
            req = requests.post(api_method, headers=self.headers, data=json.dumps(self.body))
        except ConnectionError:
            raise ConnectionError()

        if req.status_code != 200:
            raise Exception(f"Ошибка получения товаров. "
                            f"OZON request: {req.status_code}\n"
                            f"URL: {api_method}\n"
                            f"Message: {req.json()['message']}")
        req = req.json()["result"]
        return req["items"], req["total"], req["last_id"]

    def _get_products_prices_stocks(self, api: AnyStr, self_attr: List) -> None:
        """
        Метод предназначен для сбора информации о товарах по следующим api-методам:
            - v2/product/list.
            - v4/product/info/prices.
            - v3/product/info/stocks.

        Алгоритм работы:
            - В цикле создается post-запрос, который возвращает
              информацию о товарах (максимальное количество для одного запроса - 1000),
              суммарное число товаров и ID последнего товара.
            - Если cnt >= суммарного числа товаров, цикл прекращается.
            - Если cnt < суммарного числа товаров, то цикл продолжается,
              при этом на каждой итерации cnt увеличивается
              на self.body["limit"] (максимальное значение товаров внутри одного ответа от сервера Ozon).

        :param api: API-метод, который будет использован для создания post-запроса.
        :param self_attr: Атрибут экземпляра класса, в который необходимо поместить полученную информацию.
        """
        switch = True
        cnt = 0

        while switch:
            items, total, last_id = self._post_request(api)
            self_attr += items
            cnt += self.body["limit"]
            self.body["last_id"] = last_id
            switch = False if cnt >= total else True

        self.body["last_id"] = None

    def _get_products_info_list(self) -> None:
        """
        Метод предназначен для сбора информации о товарах по следующим api-методам:
            - v2/product/info/list.
        Примечание: Перед запуском уже должна быть собрана информация об ID товарах (self._get_products_prices_stocks).

        Алгоритм работы:
            - Формируется список всех ID товаров.
            - Для каждой тысячи ID (максимальное число ID для данного api-метода) создается post-запрос.
        """
        products_id = [product["product_id"] for product in self.products]
        thousands = (len(products_id) / 1000).__ceil__()
        for n_thousand in range(thousands):
            body = {"product_id": products_id[(1000 * n_thousand):(1000 * (n_thousand + 1))]}
            body = json.dumps(body)

            try:
                req = requests.post(self.urls["product_info_list"], headers=self.headers, data=body)
            except ConnectionError:
                raise ConnectionError()

            if req.status_code == 200:
                self.product_info += req.json()["result"]["items"]
            else:
                raise Exception(f"Ошибка получения товаров. "
                                f"OZON request: {req.status_code}\n"
                                f"URL: {self.urls['product_info_list']}\n"
                                f"Message: {req.json()['message']}")

    def prepare_stocks(self) -> None:
        """
        Метод предназначен для изменения формата вывода информации о складских остатках.
        """
        for note in self.stocks:
            for stock in note["stocks"]:
                note[f"{stock['type']}_present"] = stock["present"]
                note[f"{stock['type']}_reserved"] = stock["reserved"]
            note.pop("stocks")

    def full_data(self) -> List[Dict]:
        """
        Метод распаковывает полученные объекты и объединяет весь объем данных.

        :return: Список со словарями.
        """
        # Подготовка данных.
        self.product_info = flatten_dict(self.product_info)
        self.prices = flatten_dict(self.prices)
        self.prepare_stocks()

        # Объединение данных.
        result = []
        zipped = zip(self.products, self.prices, self.stocks, self.product_info)

        for prod, price, stock, info in zipped:
            data = deepcopy(prod)

            result.append(data)
            result[-1].update(price)
            result[-1].update(stock)
            result[-1].update(info)

        return result

    def run(self) -> None:
        """
        Запускает цикл сбора данных по всем api-методам.
        """
        self.products = []
        self.prices = []
        self.stocks = []
        self.product_info = []

        products_prices_stocks = list(self.urls.values())[:3]
        storages = [self.products, self.prices, self.stocks]
        zipped = zip(products_prices_stocks, storages)

        for api, attr in zipped:
            self._get_products_prices_stocks(api, attr)
        self._get_products_info_list()
