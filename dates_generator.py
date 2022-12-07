from datetime import date, timedelta
from random import random

DAYS_NUM = 50
tickers = ['USD', 'EUR', 'RUB']

if __name__ == '__main__':
    print('INSERT INTO pairs VALUES')
    for n in range(DAYS_NUM):
        date_ = (date.today() - timedelta(days=n)).strftime('%Y-%m-%d')
        for i, ticker_from in enumerate(tickers):
            for j, ticker_to in enumerate(tickers[i+1:]):
                if i == 0 and j == 0:
                    base_amount = 0.95
                    bias = 0.1 * random()
                    amount = base_amount + bias
                elif i == 0 and j == 1:
                    base_amount = 60
                    bias = 2 * random()
                    amount = base_amount + bias
                else:
                    amount = (60 + 2 * random()) / (0.95 + 0.1 * random())

                value = (date_, ticker_from, ticker_to, amount)
                print(f'    {str(value)},')