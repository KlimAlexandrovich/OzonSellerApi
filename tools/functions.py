from pandas import Timestamp, Timedelta
import asyncio
from typing import List, Dict, AnyStr
import logging
from copy import deepcopy


async def manage_asyncio_wait(pending: List[asyncio.Task]) -> None:
    """
    Функция менеджер для asyncio.wait.

    Алгоритм работы:
        - Запускает цикл, в котором ожидается выполнение задач.
        - Первая выполненная задача проверяется на возникновение исключения.
          Если исключения произошло, все остальные задачи немедленно останавливаются.
          Исключение фиксируется с помощью logging.

    :param pending: Список задач.
    """
    logging_options = [logging.basicConfig(level=logging.INFO,
                                           filename="api_requests_error.log",
                                           filemode="w",
                                           format="%(asctime)s %(levelname)s %(message)s"),
                       logging.debug("A DEBUG Message"),
                       logging.info("An INFO"),
                       logging.warning("A WARNING"),
                       logging.error("An ERROR"),
                       logging.critical("A message of CRITICAL severity")]
    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

        for done_task in done:
            if done_task.exception() is not None:
                logging.error("При выполнении запроса возникло исключение", exc_info=done_task.exception())
                [not_done_task.cancel() for not_done_task in pending]


def dates_delta(time_since: Timestamp, time_to: Timestamp) -> (int, Timedelta):
    """
    Функция принимает две даты и возвращает кортеж, содержащий:
        - Количество лет (округляет в большую сторону) между этими датами.
        - Интервальный шаг, который представляет собой разницу в секундах между двумя датами,
          деленную на количество лет между ними.
    Формула: time_to - time_since = years * interval.

    :param time_since: Начальная дата.
    :param time_to: Конечная дата.
    :return: Количество лет и длина шага.
    """
    years = ((time_to - time_since).days / 365).__ceil__()
    interval_step = (time_to - time_since) / years
    return years, interval_step


def flatten_dict(data_to_unpack: List[Dict]) -> List[Dict]:
    """
    Функция распакует словари вложенные в словари.

    Алгоритм работы:
        - На вход поступает список словарей.
        - Происходит итерация по каждому словарю из переданного списка (в дальнейшем родительский словарь).
        - Запускается цикл, который проверяет - являются ли значениями родительского словаря вложенные словари.
          Если условие True, ключ с вложенным словарем удаляется, а его ключи и значения переносятся
          в родительский словарь. При этом ключи вложенного словаря
          меняют названия в формате "удаленный ключ__ключ вложенного словаря".

    :param data_to_unpack: Список со словарями.
    :return: Список со словарями.
    """
    array = data_to_unpack.copy()
    for parent_dict in array:
        while any(isinstance(i, dict) for i in parent_dict.values()):
            for parent_key in list(parent_dict):
                if isinstance(parent_dict[parent_key], dict):
                    for nested_key, nested_value in parent_dict[parent_key].items():
                        parent_dict[parent_key + "__" + nested_key] = nested_value
                    parent_dict.pop(parent_key)
    return array


def mult_data(data: List[Dict], column: AnyStr) -> List[Dict]:
    """
    Функция для разделения данных.
    Решает проблему для вложенности следующего вида:
        - Было: List[Dict[List[Dict]]].
        - Стало: List[Dict_1, Dict_2, Dict_n].
    Примечание: Не меняет исходный список.

    Алгоритм работы:
        - Клонирует исходный словарь для каждого отдельного вложенного элемента значения и помещает в новый массив.

    :param column: Ключ, по которому будет происходить разделение.
    :param data: Список словарей со сложным уровнем вложенности.
    :return: Список словарей.
    """
    new_data = []
    for group_name in data:
        for item in range(len(group_name[column])):
            new_data.append(deepcopy(group_name))
            new_data[-1][column] = new_data[-1][column][item]
    return new_data


def mult_data2(data_to_mult: List[Dict], first_column: AnyStr, second_column: AnyStr) -> List[Dict]:
    """
    Функция для разделения данных.
    Решает проблему для вложенности следующего вида:
        - Было: List[Dict[List[Dict]]].
        - Стало: List[Dict_1, Dict_2, Dict_n].
    Примечание: Не меняет исходный список.

    Алгоритм работы:
        - Клонирует исходный словарь для каждого отдельного вложенного элемента значения и помещает в новый массив.
    :param data_to_mult: Список словарей со сложным уровнем вложенности.
    :param first_column: Ключ, по которому будет происходить разделение (ставится в соответствии с second_column).
    :param second_column: Ключ, по которому будет происходить разделение (ставится в соответствии с first_column).
    :return: Список словарей.
    """
    new_data = []
    for note in data_to_mult:

        if len(note[first_column]) != len(note[second_column]):
            for i1 in range(len(note[first_column])):
                new_data.append(deepcopy(note))
                new_data[-1][second_column] = None
                new_data[-1][first_column] = new_data[-1][first_column][i1]

        elif len(note[first_column]) == len(note[second_column]):  # 1 < len(note['products'])
            for i1 in range(len(note[first_column])):
                new_data.append(deepcopy(note))
                new_data[-1][second_column] = new_data[-1][second_column][i1]
                new_data[-1][first_column] = new_data[-1][first_column][i1]

    return new_data


def flatten_list(array: List[List]) -> List:
    """
    Функция распакует вложенные списки.

    :param array: Список с вложенными списками.
    :return: Одномерный список.
    """
    output_array = deepcopy(array)
    while any(isinstance(i, list) for i in output_array):
        for value in output_array:
            if isinstance(value, list):
                for_append = value
                output_array.remove(value)
                output_array += for_append
    return output_array
