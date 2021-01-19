import pandas
from datetime import datetime, timedelta
from src.sql import MSSQL
from src.iex import get_history
from dotenv import load_dotenv

load_dotenv()

def get_equities():
    with MSSQL() as db:
        sql = """
            SELECT symbol, EquityId, RefreshHistory_DataRange
            FROM dbo.Equity
            WHERE RefreshHistoryData = 1
        """
        return db.query(sql)

def reset_flag(symbols):
    with MSSQL() as db:
        sql = f"""
            UPDATE dbo.Equity
            SET RefreshHistoryData = 0
            WHERE symbol IN ({','.join(['?'] * len(symbols))})
        """
        joined = "', '".join(symbols)
        return db.execute(sql, tuple(symbols))

# reset_flag(('AAPL', 'ADBE'))

def extract():
    equity_rows = get_equities()
    end = datetime.now() - timedelta(days=1)
    history = []
    columns = ['symbol', 'open', 'close', 'high', 'low', 'volume']
    if equity_rows:
        for row in equity_rows:
            symbol, equity_id, dateRange = row
            start = end
            if dateRange.endswith('m'):
                months = int(dateRange[:-1])
                start = start - timedelta(days=months*28)
            elif dateRange.endswith('y'):
                years = int(dateRange[:-1])
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
        }, inplace = True) 
        return history

def load(data):
    with MSSQL() as db:
        data.to_sql('Stage_EquityHistory', schema='dbo', con=db.get_engine(), if_exists='append', index=True, index_label='TradeDate')

data = extract()
load(data)