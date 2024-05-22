import asyncio
from aiohttp import ClientSession
import json
from pandas import to_datetime, Timestamp
from dateutil.relativedelta import relativedelta
from ..tools.functions import manage_asyncio_wait


class AsyncFinanceRealizationList:
    """
    Асинхронный класс для получения данных о финансовых операциях (начисления, расходы и др.) с Ozon.
    """

    def __init__(self, headers: dict, date_since: str, date_to: str) -> None:
        """
        Используемый api-метод: v3/finance/transaction/list.
        Ссылка на документацию к методу:
        https://docs.ozon.ru/api/seller/#operation/FinanceAPI_FinanceTransactionListV3.
        Полученные данные хранятся в следующих атрибутах:
            - self.data.

        Алгоритм работы:
            - Начиная с даты date_since создаются задачи
              по получению информации о транзакциях с временным интервалом (date_since, date_since + 1 month),
              при этом с каждой итерацией date_since увеличивается на 1 month 1 day
              (1 day добавлен, чтобы избежать получения повторных транзакций).
            - Внутри каждой задачи (self._fetch) отправляются post-запросы для получения данных.

        :param headers: Параметры для входа в личный кабинет: {"Client-Id": "string", "Api-Key": "string"}.
        :param date_since: Период с которого происходит сбор транзакций в формате: %Y.%m.%d.
        :param date_to: Период по который происходит сбор транзакций в формате: %Y.%m.%d или "today".
        """
        self.headers = headers
        self.url = "https://api-seller.ozon.ru/v3/finance/transaction/list"
        self.since = to_datetime(date_since)
        self.to = to_datetime(date_to)
        self.formats = ["%Y-%m-%dT%XZ", "%Y-%m"]
        self.transaction_type = "all"
        self.data = []

    def _request_body(self, date_since: str, date_to: str, page: int) -> json:
        """
        Метод создает тело для post-запроса к Ozon.
        Шаблон тела определяется api-методом.

        :param date_since: Период с которого происходит сбор транзакций в текущей задаче в формате: %Y.%m.%d.
        :param date_to: Период по который происходит сбор транзакций в текущей задаче в формате: %Y.%m.%d или "today".
        :param page: Указатель на страницу, с которой необходимо получить данные.
        :return: JSON объект.
        """
        body = {
            "filter": {
                "date": {
                    "from": date_since,
                    "to": date_to
                },
                "operation_type": [],
                "posting_number": "",
                "transaction_type": self.transaction_type
            },
            "page": page,
            "page_size": 1000
        }
        # Для отладки.
        print(body["filter"]["date"]["from"], " --> ", body["filter"]["date"]["to"], body["page"])
        body = json.dumps(body)
        return body

    async def _fetch(self, session: ClientSession, date_since: Timestamp, date_to: Timestamp) -> None:
        """
        Метод в цикле создает post-запросы к Ozon за указанный период.
        За статус работы цикла отвечает параметр switcher.

        Алгоритм работы:
            - Запускается цикл для отправки post-запросов.
            - В ответ от сервера приходят запрашиваемые информация о транзакциях
              и количество страниц с данными - page_count, при этом за один
              запрос можно получить данные только с одной страницы.
            - Цикл работает до тех пор, пока параметр page меньше page_count, иначе switcher = False.

        :param session: Экземпляр класса ClientSession.
        :param date_since: Период с которого происходит сбор транзакций в текущей задаче.
        :param date_to: Период по который происходит сбор транзакций в текущей задаче.
        """
        page = 1
        switcher = True

        since_strf = date_since.strftime(self.formats[0])
        to_strf = date_to.strftime(self.formats[0])

        while switcher:
            body = self._request_body(since_strf, to_strf, page)

            async with session.post(self.url, data=body) as result:
                status_code = result.status
                result = await result.json()
                if result.get("message"):
                    error_message = result["message"]
                    raise Exception(f"Ошибка подключения к серверу. "
                                    f"Код ошибки: {status_code}. "
                                    f"Message: {error_message}")
                else:
                    self.data += result["result"]["operations"]
                    switcher = False if page == result["result"]["page_count"] else True
                    page += 1

    async def _main(self) -> None:
        """
        Тело цикла событий.
        Создает задачи для получения данных.
        Временной интервал для запроса - 1 месяц.
        """
        async with ClientSession(headers=self.headers) as session:
            start = self.since
            pending = []
            while start < self.to:
                date_to = start + relativedelta(months=+1)

                pending.append(asyncio.create_task(self._fetch(session, start, date_to)))
                start += relativedelta(months=+1, days=+1)

            await manage_asyncio_wait(pending)

    def run(self) -> None:
        """
        Запускает цикл событий.
        """
        self.data = []
        asyncio.run(self._main())

    async def run_in_jupyter(self) -> None:
        """
       Запускает цикл событий.
       Метод для отладки и работы в среде Jupyter.
       """
        self.data = []
        await self._main()
