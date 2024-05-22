from typing import List, Dict, AnyStr
import requests


def get_ozon_warehouses(headers: Dict, output_format: AnyStr = "full") -> List[Dict] or Dict or List:
    """
    Функция для получения информации о текущих складах Ozon.
    Используемый api-метод: v1/supplier/available_warehouses.
    Ссылка на документацию к методу:
    https://docs.ozon.ru/api/seller/#operation/SupplierAPI_SupplierAvailableWarehouses.

    :param output_format: Объем информации о действующих складах;
                          "full" -> List[Dict],
                          "id_name" -> Dict,
                          "id" -> List.
    :param headers: Параметры для входа в личный кабинет: {"Client-Id": "string", "Api-Key": "string"}.
    :return: В зависимости от выбора формата выходных данных возвращает соответствующую информацию о складах.
    """
    api_method = "https://api-seller.ozon.ru/v1/supplier/available_warehouses"
    try:
        wh = requests.get(url=api_method, headers=headers)
    except ConnectionError:
        raise ConnectionError
    if wh.status_code != 200:
        raise ConnectionError

    wh = wh.json()["result"]
    if output_format == "full":
        return wh
    elif output_format == "id_name":
        return {wh_id["warehouse"]["id"]: wh_id["warehouse"]["name"] for wh_id in wh}
    elif output_format == "id":
        return [wh_id["warehouse"]["id"] for wh_id in wh]
