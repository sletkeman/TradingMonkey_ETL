from datetime import datetime
import pandas
from src.sql import MSSQL
from src.iex import get_quote
from src.util import chunker

def get_symbols():
    with MSSQL() as db:
        return pandas.read_sql("Exec usp_Equity_Read", db.get_connection())

def get_date_id(date):
    with MSSQL() as db:
        sql = f"SELECT TradedateID FROM dbo.TradeDate WHERE TradeDate = '{date}'"
        rows = db.query_one(sql)
        return rows[0]

def extract(date, logger):
    logger.info(f"Starting Trading Monkey Extract")
    logger.info(f"Date: {date}")
    date_id = get_date_id(date)
    logger.info(f"DateId: {date_id}")
    # get the symbols
    symbols = get_symbols()
    symbols['Symbol'] = symbols['Symbol'].str.upper()
    symbol_list = symbols['Symbol'].tolist()

    # request quotes 100 at a time
    chunks = chunker(symbol_list, 100)
    quote_columns = ['symbol', 'iexOpen', 'iexClose', 'high', 'low', 'iexVolume']
    quotes = []
    for chunk in chunks:
        quotes.append(get_quote(chunk, quote_columns))
    quotes = pandas.concat(quotes)

    # merge it into one dataframe and clean it up 
    data = symbols[['EquityID', 'Symbol']].merge(quotes, left_on='Symbol', right_on='symbol', how='outer')
    data.rename(columns = {
        'iexOpen': 'Open', 
        'iexClose': 'Close', 
        'high': 'High',
        'low': 'Low',
        'iexVolume': 'Volume'
    }, inplace = True) 
    data.drop(columns=['symbol'], inplace = True)
    data['TradeDate'] = date
    data['TradeDateID'] = date_id
    data['LastModifiedDate'] = datetime.now()
    logger.info(f"{len(data.index)} records found")
    return data
    
def load(data, logger):
    logger.info(f"Starting Trading Monkey Load")

    with MSSQL() as db:
        data.to_sql('Stage_IEXData', schema='dbo', con=db.get_engine(), if_exists='append', index=False)
