import requests
import json
from typing import Dict


class SellerRating:
    """
    Класс для получения актуальной информации о рейтинге магазина.
    """

    def __init__(self, headers: Dict) -> None:
        """
        Используемый api-метод: v1/rating/summary.
        Ссылка на документацию к методу: https://docs.ozon.ru/api/seller/#operation/RatingAPI_RatingSummaryV1.
        Полученные данные хранятся в следующих атрибутах:
            - self.data.
            - self.penalty_score_exceeded.
            - self.premium.
            - self.localization_index.

        :param headers: Параметры для входа в личный кабинет: {"Client-Id": "string", "Api-Key": "string"}.
        """
        self.headers = headers
        self.url = "https://api-seller.ozon.ru/v1/rating/summary"
        self.data = None
        self.penalty_score_exceeded = None
        self.premium = None
        self.localization_index = None

    def post_request(self) -> Dict:
        """
        Функция отправляет запрос к серверу Ozon.

        :return: Словарь с информацией о рейтинге магазина.
        """
        try:
            req = requests.post(self.url, headers=self.headers, data=json.dumps({}))
        except ConnectionError:
            raise ConnectionError
        if req.status_code != 200:
            raise ConnectionError
        req = req.json()
        return req

    # def mult_data(self):
    #     if self.data:
    #         new_data_ = []
    #         for group_name_ in self.data:
    #             for item_ in range(len(group_name_['items'])):
    #                 new_data_.append(group_name_.copy())
    #                 new_data_[-1]['items'] = new_data_[-1]['items'][item_]
    #         self.data = new_data_
    #     return

    def run(self) -> None:
        """
        Запускает self.post_request, после чего полученные данные распределяются по нескольким категориям.
        """
        req = self.post_request()
        self.data = req["groups"]
        # self.mult_data()
        # self.data = functions.flatten_dict(self.data)
        self.penalty_score_exceeded = req["penalty_score_exceeded"]
        self.premium = req["premium"]
        self.localization_index = req["localization_index"]
