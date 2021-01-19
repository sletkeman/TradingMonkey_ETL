from iexfinance.stocks import Stock, get_historical_data


def get_quote(symbols, columns):
    batch = Stock(symbols, output_format="pandas")
    quote = batch.get_quote()
    return quote[columns]

def is_market_open():
    appl = Stock(['AAPL'], output_format="json").get_quote()
    return appl.get('isUSMarketOpen')

def get_history(symbol, start, end, columns):
    history = get_historical_data(symbol, start=start, end=end, output_format='pandas')
    return history[columns]
