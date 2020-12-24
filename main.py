import os
from dotenv import load_dotenv
from src.sql import MSSQL
from src.iex import get_quote
from src import util
from datetime import datetime
import pandas

def get_symbols():
    with MSSQL() as db:
        return pandas.read_sql("Exec usp_Equity_Read", db.get_connection())
def get_date_id():
    with MSSQL() as db:
        # today = '2020-12-16'
        today = datetime.today().strftime('%Y-%m-%d')
        sql = f"SELECT TradedateID FROM dbo.TradeDate WHERE TradeDate = '{today}'"
        rows = db.query_one(sql)
        return rows[0]

def extract(logger):
    logger.info(f"Starting Trading Monkey Extract")
    date_id = get_date_id()

    # get the symbols
    symbols = get_symbols()
    symbols['Symbol'] = symbols['Symbol'].str.upper()
    symbol_list = symbols['Symbol'].tolist()

    # request quotes 100 at a time
    chunks = util.chunker(symbol_list, 100)
    quote_columns = ['symbol', 'open', 'close', 'high', 'low', 'volume']
    quotes = []
    for chunk in chunks:
        quotes.append(get_quote(chunk, quote_columns))
    quotes = pandas.concat(quotes)

    # merge it into one dataframe and clean it up 
    data = symbols[['EquityID', 'Symbol']].merge(quotes, left_on='Symbol', right_on='symbol', how='outer')
    data.rename(columns = {
        'open': 'Open', 
        'close': 'Close', 
        'high': 'High',
        'low': 'Low',
        'volume': 'Volume'
    }, inplace = True) 
    data.drop(columns=['symbol'], inplace = True)
    data['TradeDate'] = datetime.today().strftime('%Y-%m-%d')
    data['TradeDateID'] = date_id
    data['LastModifiedDate'] = datetime.now()
    return data
    
def load(data, logger):
    logger.info(f"Starting Trading Monkey Load")

    with MSSQL() as db:
        data.to_sql('Stage_IEXData', schema='dbo', con=db.get_engine(), if_exists='append', index=False)

if __name__ == '__main__':
    load_dotenv()

    # move to working directory...
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    logger = util.setup_logger_stdout('tradingMonkey_ETL')

    data = extract(logger)
    load(data, logger)