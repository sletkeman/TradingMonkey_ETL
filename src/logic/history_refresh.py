"""
    Defines the functions to be used in the history service
"""

from datetime import datetime, timedelta
import pandas
from lib.db import get_equities
from lib.iex import get_history
from lib.util import MSSQL

def extract():
    equity_rows = get_equities()
    end = datetime.now() - timedelta(days=1)
    history = []
    columns = ['symbol', 'open', 'close', 'high', 'low', 'volume']
    if equity_rows:
        for row in equity_rows:
            symbol, equity_id, time_span = row
            start = end
            if time_span.endswith('m'):
                months = int(time_span[:-1])
                start = start - timedelta(days=months*28)
            elif time_span.endswith('y'):
                years = int(time_span[:-1])
                start = start.replace(year=end.year-years)
            else:
                break

            data = get_history(symbol, start, end, columns)
            data['EquityId'] = equity_id
            history.append(data)

        history = pandas.concat(history)
        history.rename(columns = {
            'symbol': 'Symbol',
            'open': 'Open',
            'close': 'Close',
            'high': 'High',
            'low': 'Low',
            'volume': 'Volume'
            },
            inplace = True
        )
        return history

def load(data):
    with MSSQL() as db:
        data.to_sql(
            'Stage_EquityHistory',
            schema='dbo',
            con=db.get_engine(),
            if_exists='append',
            index=True,
            index_label='TradeDate'
        )
