"""
    Database calls
"""

import pandas
from lib.util import MSSQL

def get_symbols():
    """Call the stored procedure that gets the symbols and returns it as a pandas dataframe"""
    with MSSQL() as db:
        return pandas.read_sql('Exec usp_Equity_Read', db.get_connection())

def get_date_id(date):
    with MSSQL() as db:
        sql = "SELECT TradedateID FROM dbo.TradeDate WHERE TradeDate = ?"
        result = db.query_one(sql, (date,))
        if result:
            return result[0]
        else:
            raise Exception(f"A tradeDateID could not be found for {date}")

def get_equities_needing_refresh():
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
        return db.execute(sql, tuple(symbols))
