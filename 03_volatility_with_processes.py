# -*- coding: utf-8 -*-
import multiprocessing
# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью в МНОГОПРОЦЕССНОМ стиле
#
# Бумаги с нулевой волатильностью вывести отдельно.
# Результаты вывести на консоль в виде:
#   Максимальная волатильность:
#       ТИКЕР1 - ХХХ.ХХ %
#       ТИКЕР2 - ХХХ.ХХ %
#       ТИКЕР3 - ХХХ.ХХ %
#   Минимальная волатильность:
#       ТИКЕР4 - ХХХ.ХХ %
#       ТИКЕР5 - ХХХ.ХХ %
#       ТИКЕР6 - ХХХ.ХХ %
#   Нулевая волатильность:
#       ТИКЕР7, ТИКЕР8, ТИКЕР9, ТИКЕР10, ТИКЕР11, ТИКЕР12
# Волатильности указывать в порядке убывания. Тикеры с нулевой волатильностью упорядочить по имени.
#
import os
import time
from multiprocessing import Process, Pipe



def timer_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Функция {func.__name__} выполнилась за {execution_time} секунд.")
        return result

    return wrapper


class TickerVolatility:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.tickers = []
        self.volatilities = {}

    def calculate_volatility(self, prices):
        if len(prices) == 0:
            return 0

        min_price = min(prices)
        max_price = max(prices)
        average_price = (max_price + min_price) / 2
        volatility = ((max_price - min_price) / average_price) * 100

        return volatility

    def process_file(self, file_path, conn):
        ticker = os.path.basename(file_path).split(".")[0]
        prices = []

        with open(file_path, "r") as file:
            next(file)  # skip header line
            for line in file:
                _, _, price, _ = line.strip().split(",")
                prices.append(float(price))

        volatility = self.calculate_volatility(prices)
        if volatility == 0:
            self.tickers.append(ticker)
        else:
            self.volatilities[ticker] = volatility

        conn.send((ticker, volatility))
        conn.close()

    @staticmethod
    def worker(file_path, conn):
        ticker_volatility = TickerVolatility('')
        ticker_volatility.process_file(file_path, conn)

    @staticmethod
    def merge_results(results):
        tickers = []
        volatilities = {}

        for result in results:
            ticker, volatility = result
            if volatility == 0:
                tickers.append(ticker)
            else:
                volatilities[ticker] = volatility

        return tickers, volatilities

    @staticmethod
    def run_parallel(file_paths):
        parent_conns, child_conns = zip(*[Pipe() for _ in range(len(file_paths))])
        processes = []

        for i, file_path in enumerate(file_paths):
            process = Process(target=TickerVolatility.worker, args=(file_path, child_conns[i]))
            processes.append(process)
            process.start()

        for conn in child_conns:
            conn.close()

        results = [parent_conn.recv() for parent_conn in parent_conns]
        tickers, volatilities = TickerVolatility.merge_results(results)

        for process in processes:
            process.join()

        return tickers, volatilities
    @timer_decorator
    def run(self):
        files = os.listdir(self.folder_path)
        file_paths = [os.path.join(self.folder_path, file) for file in files]

        tickers, volatilities = TickerVolatility.run_parallel(file_paths)

        self.tickers = tickers
        self.volatilities = volatilities

        self.tickers.sort()
        sorted_volatilities = sorted(self.volatilities.items(), key=lambda x: x[1], reverse=True)
        max_volatilities = sorted_volatilities[:3]
        min_volatilities = sorted_volatilities[-3:]

        print("Максимальная волатильность:")
        for ticker, volatility in max_volatilities:
            print(f"    {ticker} - {volatility:.2f} %")

        print("Минимальная волатильность:")
        for ticker, volatility in min_volatilities:
            print(f"    {ticker} - {volatility:.2f} %")

        print("Нулевая волатильность:")
        for ticker in self.tickers:
            print(f"    {ticker}")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    ticker_volatility = TickerVolatility("trades")
    ticker_volatility.run()