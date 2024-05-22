class APIError(Exception):
    def __init__(self, message, status_code, api_method):
        message = (f"Ошибка получения данных. OZON request: {status_code}. API: {api_method}.\n"
                   f"Message: {message}")
        super().__init__(message)


class APIConnectionError(ConnectionError):
    def __init__(self):
        message = f"Ошибка подключения к сети."
        super().__init__(message)
