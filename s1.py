from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import time
import os
import requests
import pandas as pd


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


def get_data(symbol):
    result = requests.get(url)
    return result


def execute_on_process_pool(function, arguments, max_workers=None):
    print(21 * "-")
    print('Process Pool Executor')
    print('function: {}'.format(function))
    print('arguments: {}'.format(arguments))
    print('max workers: {}'.format(max_workers))
    # only picklable objects can be executed and returned
    with ProcessPoolExecutor(max_workers=None) as executor:
        for result in executor.map(function, arguments):
            print(result)


def execute_on_thread_pool(function, arguments, max_workers=1):
    print(21 * "-")
    print('Thread Pool Executor')
    print('function: {}'.format(function))
    print('arguments: {}'.format(arguments))
    print('max workers: {}'.format(max_workers))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(function, arguments):
            print(result)


def work_on_csv(function, arguments, max_workers=1):
    print(21 * "-")
    print('Thread Pool Executor')
    print('function: {}'.format(function))
    print('arguments: {}'.format(arguments))
    print('max workers: {}'.format(max_workers))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(function, arguments):
            print(result)


if __name__ == '__main__':

    arguments = range(8)
    execute_on_process_pool(square, arguments)

    arguments = [
        {'a': 1, 'b': 2, 'x': 4},
        {'a': 1, 'b': 2, 'x': 6}
    ]
    execute_on_process_pool(lin, arguments, max_workers=8)
    execute_on_thread_pool(lin, arguments, max_workers=8)
