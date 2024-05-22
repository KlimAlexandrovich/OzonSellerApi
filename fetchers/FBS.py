import asyncio
from aiohttp import ClientSession
import json
from pandas import to_datetime, offsets, Timestamp
from ..tools.functions import dates_delta, manage_asyncio_wait


class FBSPostingList:
    """
    Асинхронный класс для получения данных о заказах Ozon по модели FBS.
    """

    def __init__(self, headers: dict, date_since: str, date_to: str, status: str = "") -> None:
        """
        Используемый api-метод: v3/posting/fbs/list.
        Ссылка на документацию к методу: https://docs.ozon.ru/api/seller/#operation/PostingAPI_GetFbsPostingListV3.
        Полученные данные хранятся в следующих атрибутах:
            - self.data.

        Алгоритм работы:
            - Временной интервал (за который необходимо получить данные) дробится
              на несколько частей с максимальной длиной - один год.
            - Для каждого нового интервала создается отдельная задача self._fetch внутри цикла.
            - Внутри каждой задачи (self._fetch) отправляются post-запросы для получения данных.

        :param headers: Параметры для входа в личный кабинет: {"Client-Id": "string", "Api-Key": "string"}.
        :param date_since: Период с которого происходит сбор заказов в формате: %Y.%m.%d.
        :param date_to: Период по который происходит сбор заказов в формате: %Y.%m.%d или "today".
        :param status: Статус заказов; по умолчанию "" возвращает все заказы.
        """
        self.headers = headers
        self.url = "https://api-seller.ozon.ru/v3/posting/fbs/list"
        self.date_since = to_datetime(date_since)
        self.date_to = to_datetime(date_to)
        self.date_format = "%Y-%m-%dT%XZ"
        self.status = status
        self.years, self.interval = dates_delta(self.date_since, self.date_to)
        self.data = None

    def _request_body(self, date_since: str, date_to: str, offset: int) -> json:
        """
        Метод создает тело для post-запроса к Ozon.
        Шаблон тела определяется api-методом.

        :param date_since: Период с которого происходит сбор заказов в текущей задаче в формате: %Y.%m.%d.
        :param date_to: Период по который происходит сбор заказов в текущей задаче в формате: %Y.%m.%d или "today".
        :param offset: Отвечает за количество заказов, которые необходимо пропустить из ответа.
        :return: JSON объект.
        """
        body = {
            "dir": "ASC",
            "filter": {
                "since": date_since,
                "status": self.status,
                "to": date_to
            },
            "limit": 1000,
            "offset": offset,
            "translit": True,
            "with": {
                "analytics_data": True,
                "financial_data": True
            }
        }
        # Для отладки.
        print(body["filter"]["since"], " --> ", body["filter"]["to"], body["offset"])
        body = json.dumps(body)
        return body

    async def _fetch(self, session: ClientSession, since: Timestamp) -> None:
        """
        Метод в цикле создает post-запросы к Ozon за указанный период.
        За статус работы цикла отвечает параметр switcher=has_next.

        Алгоритм работы:
        - Цикл работает до тех пор, пока в ответе
        приходит параметр has_next = True или пока не произойдет исключение.
        - Если has_next = True, то
        параметр offset (отвечает за количество заказов, которые необходимо пропустить из ответа)
        увеличивается на 1000 (максимальное количество заказов, которое можно получить за один запрос).
        - Если has_next = False, то цикл прекращается.

        :param session: Экземпляр класса ClientSession.
        :param since: Период с которого происходит сбор заказов в текущей задаче.
        """
        offset = 0
        has_next = True

        since_strf = since.strftime(self.date_format)
        to_strf = (since + self.interval).strftime(self.date_format)

        while has_next:
            body = self._request_body(since_strf, to_strf, offset)

            async with session.post(self.url, data=body) as result:
                status_code = result.status
                result = await result.json()

                if result.get("message"):
                    error_message = result["message"]
                    raise Exception(f"Ошибка подключения к серверу. "
                                    f"Код ошибки: {status_code}. "
                                    f"Message: {error_message}")

                else:
                    self.data += result["result"]["postings"]
                    has_next = result["result"]["has_next"]
                    offset += 1000

    async def _main(self) -> None:
        """
        Тело цикла событий.
        """
        async with ClientSession(headers=self.headers) as session:
            pending = []
            since = self.date_since

            for year in range(self.years):
                pending.append(asyncio.create_task(self._fetch(session, since)))
                since += self.interval + offsets.Second(1)

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
