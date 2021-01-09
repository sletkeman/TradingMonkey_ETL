from iexfinance.stocks import Stock

def get_quote(symbols, columns):
    batch = Stock(symbols, output_format="pandas")
    quote = batch.get_quote()
    return quote[columns]

def is_market_open():
    appl = Stock(['AAPL'], output_format="json").get_quote()
    return appl.get('isUSMarketOpen')