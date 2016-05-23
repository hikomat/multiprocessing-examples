from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import os
import requests
import pandas as pd
from io import StringIO


def square(x):
    pid = os.getpid()
    print("Parameters: {}. (PID={})".format(x, pid))
    return x * x


def lin(params):
    pid = os.getpid()
    a = params.get('a')
    b = params.get('b')
    x = params.get('x')
    print("y = {}*{} + {}. (PID={})".format(a, x, b, pid))
    return a * x + b


def get_data(params):
    symbol, start_date, end_date = params.get('symbol'), params.get('start_date'), params.get('end_date')
    s_year, s_month, s_day = start_date.split('-')
    e_year, e_month, e_day = end_date.split('-')
    data = {
        's': symbol,
        'a': s_month,
        'b': s_day,
        'c': s_year,
        'd': e_month,
        'e': e_day,
        'f': e_year,
        'g': 'd',
        'ignore': '.csv',
    }
    url = 'http://real-chart.finance.yahoo.com/table.csv'
    print(data)
    result = requests.get(url, params=data)
    d = pd.read_csv(StringIO(result.text))
    d.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'adjclose']
    return d


def execute_on_process_pool(function, arguments, max_workers=None):
    print(21 * "-")
    print('Process Pool Executor')
    print('function: {}'.format(function))
    print('arguments: {}'.format(arguments))
    print('max workers: {}'.format(max_workers))
    # only picklable objects can be executed and returned
    results = []
    with ProcessPoolExecutor(max_workers=None) as executor:
        for result in executor.map(function, arguments):
            print(result)
            results.append(result)
    return results


def execute_on_thread_pool(function, arguments, max_workers=1):
    print(21 * "-")
    print('Thread Pool Executor')
    print('function: {}'.format(function))
    print('arguments: {}'.format(arguments))
    print('max workers: {}'.format(max_workers))
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(function, arguments):
            print(result)
    return results


if __name__ == '__main__':

    # arguments = range(8)
    # execute_on_process_pool(square, arguments)

    # arguments = [
    #     {'a': 1, 'b': 2, 'x': 4},
    #     {'a': 1, 'b': 2, 'x': 6}
    # ]
    # execute_on_process_pool(lin, arguments, max_workers=8)
    # execute_on_thread_pool(lin, arguments, max_workers=8)

    arguments = [
        {'start_date': '2015-01-01', 'end_date': '2016-01-01', 'symbol': 'GOOG'},
        {'start_date': '2015-01-01', 'end_date': '2016-01-01', 'symbol': 'F'},
    ]
    execute_on_process_pool(get_data, arguments, max_workers=8)