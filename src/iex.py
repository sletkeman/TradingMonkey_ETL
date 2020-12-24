from iexfinance.stocks import Stock

def get_quote(symbols, columns):
    batch = Stock(symbols, output_format="pandas")
    quote = batch.get_quote()
    return quote[columns]
