from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import os
import requests
import pandas as pd
from io import StringIO
import numpy as np
import time
from itertools import combinations


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


def get_symbols():
    with open('symbols') as s:
        symbols = s.read().strip().split()
    return symbols


def get_data(params):
    symbol, start_date, end_date = params.get('symbol'), params.get('start_date'), params.get('end_date')
    s_year, s_month, s_day = start_date.split('-')
    e_year, e_month, e_day = end_date.split('-')
    # http://real-chart.finance.yahoo.com/table.csv?s=GOOG&a=07&b=19&c=2015&d=04&e=24&f=2016&g=d&ignore=.csv
    data = {
        's': symbol,
        'a': str(int(s_month) - 1).zfill(2),
        'b': s_day,
        'c': s_year,
        'd': str(int(e_month) - 1).zfill(2),
        'e': e_day,
        'f': e_year,
        'g': 'd',
        'ignore': '.csv',
    }
    url = 'http://real-chart.finance.yahoo.com/table.csv?s={s}&a={a}&b={b}&c={c}&d={d}&e={e}&f={f}&g=d&ignore=.csv'.format(
        **data)
    result = requests.get(url)
    if result.text[0] == '<':
        return None
    else:
        d = pd.read_csv(StringIO(result.text), warn_bad_lines=True, error_bad_lines=False)
        d.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'adjclose']
        d['symbol'] = symbol
        d.index = pd.to_datetime(d.date)
        d.sort_index(inplace=True)
        del d['date']
        return d


def execute_on_process_pool(function, arguments, max_workers=None):
    t0 = time.time()
    print(21 * "-")
    print('Process Pool Executor')
    print('Function to execute: {}'.format(function))
    print('Max workers: {}'.format(max_workers))
    # only picklable objects can be executed and returned
    results = []
    with ProcessPoolExecutor(max_workers=None) as executor:
        for result in executor.map(function, arguments):
            results.append(result)
    print("Elapsed: {:.4f}".format(time.time() - t0))
    return results


def execute_on_thread_pool(function, arguments, max_workers=1):
    t0 = time.time()
    print(21 * "-")
    print('Thread Pool Executor')
    print('Function to execute: {}'.format(function))
    print('Max threads: {}'.format(max_workers))
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(function, arguments):
            results.append(result)
    print("Elapsed: {:.4f}".format(time.time() - t0))
    return results


def get_historical_prices(symbol_list):
    arguments = []
    for symbol in symbol_list:
        arguments.append({'start_date': '2015-01-01', 'end_date': '2016-01-01', 'symbol': symbol})
    results = execute_on_thread_pool(get_data, arguments, max_workers=8)
    return list(filter(lambda x: x is not None, results))


def quotes(symbols):
    s = '+'.join(symbols)
    url = 'http://finance.yahoo.com/d/quotes.csv?s={}&f=snabopghc1l1'.format(s)
    result = requests.get(url)
    return result.text


def compute_stats(dataframes):
    results = execute_on_thread_pool(stats, dataframes, max_workers=8)
    return pd.DataFrame(results)


def stats(d):
    r = d.adjclose / d.adjclose.shift(1)
    return pd.Series({
        'r_std': r.std(),
        'r_mean': r.mean(),
        'symbol': d.symbol.iloc[0]
    })


def correlate_returns(dataframes):
    c = combinations(dataframes, 2)
    correlation_series = execute_on_process_pool(corr, c, max_workers=8)
    correlation_df = pd.DataFrame(correlation_series)
    return correlation_df
    # return pd.pivot_table(correlation_df, 'r', 's1', 's2', fill_value=None)


def corr(combination):
    d1, d2 = combination
    r1 = d1.adjclose / d1.adjclose.shift(1)
    r2 = d2.adjclose / d2.adjclose.shift(1)
    return pd.Series({
        's1': d1.symbol.iloc[0],
        's2': d2.symbol.iloc[0],
        'r': r1.corr(r2)
    })


if __name__ == '__main__':

    arguments = range(8)
    execute_on_process_pool(square, arguments)

    arguments = [
        {'a': 1, 'b': 2, 'x': 4},
        {'a': 1, 'b': 2, 'x': 6}
    ]
    execute_on_process_pool(lin, arguments, max_workers=8)
    execute_on_thread_pool(lin, arguments, max_workers=8)

    # Loading symbols
    symbols = get_symbols()

    # Randomize
    np.random.shuffle(symbols)

    # Download data
    dataframes = get_historical_prices(symbols[0:128])
    print("Downloaded {}".format(len(dataframes)))

    # Compute stats with thread
    returns_and_sds = compute_stats(dataframes)
    print(returns_and_sds)

    c = correlate_returns(dataframes)
    print(c)
