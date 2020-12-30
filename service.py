import time
import random
from pathlib import Path
from SMWinservice import SMWinservice
from src import util

class TradingMonkey_ETL(SMWinservice):
    _svc_name_ = "TradingMonkey_ETL"
    _svc_display_name_ = "Trading Money ETL"
    _svc_description_ = "Extracts stock quotes from the IEX API and loads them to MSSQL."

    def start(self):
        self.isrunning = True

    def stop(self):
        self.isrunning = False

    def main(self):
        logger = util.setup_logger_stdout('tradingMonkey_ETL')
        while self.isrunning:
            random.seed()
            x = random.randint(1, 1000000)
            logger.info(f"monkey {x}")
            time.sleep(5)

if __name__ == '__main__':
    TradingMonkey_ETL.parse_command_line()