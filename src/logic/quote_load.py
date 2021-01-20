"""
    Defines the functions to be used in the quote service
"""

from datetime import datetime
import pandas
from lib.db import get_date_id, get_symbols
from lib.iex import get_quote
from lib.util import chunker, MSSQL

def extract(logger):
    logger.info("Starting Trading Monkey Extract")
    date = datetime.today().strftime('%Y-%m-%d')
    date_id = get_date_id(date)
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
    data = symbols[['EquityID', 'Symbol']].merge(
        quotes,
        left_on='Symbol',
        right_on='symbol',
        how='outer'
    )
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
    logger.info("Starting Trading Monkey Load")

    with MSSQL() as db:
        data.to_sql(
            'Stage_IEXData',
            schema='dbo',
            con=db.get_engine(),
            if_exists='append',
            index=False
        )
