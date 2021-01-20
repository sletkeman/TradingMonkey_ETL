"""
    Defines the functions that interact with IEX Finance
"""

from iexfinance.stocks import Stock, get_historical_data

def get_quote(symbols, columns):
    """Gets the stock quote matching the given symbols"""
    batch = Stock(symbols, output_format="pandas")
    quote = batch.get_quote()
    return quote[columns]

def is_market_open():
    """Queries whether the market is open by checking Apple's quote"""
    appl = Stock(['AAPL'], output_format="json").get_quote()
    return appl.get('isUSMarketOpen')

def get_history(symbol, start, end, columns):
    """Gets historical data matching the symbol, start and end dates"""
    history = get_historical_data(symbol, start=start, end=end, output_format='pandas')
    return history[columns]
