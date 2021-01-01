import servicemanager
import socket
import sys
import win32event
import win32service
import win32serviceutil
from dotenv import load_dotenv
from datetime import datetime
from os import environ
from src import util
import etl

class TradingMonkey_ETL(win32serviceutil.ServiceFramework):
    _svc_name_ = 'TradingMonkey_ETL'
    _svc_display_name_ = 'Trading Monkey ETL'
    _svc_description_ = 'Extracts stock quotes from the IEX API and loads them to MSSQL'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)

    def SvcDoRun(self):
        # file_path = 'C:\\Users\\Scott\\TradingMonkey_ETL\\TradingMonkey_ETL.log'
        file_path = 'C:\\TradingMonkey_ETL.log'
        logger = util.setup_logger('tradingMonkey_ETL', file_path)
        try:
            load_dotenv(dotenv_path='C:\\Users\\Scott\\TradingMonkey_ETL\\.env')
            # iex_token = [environ[key] for key in environ if key == 'IEX_TOKEN']
            # logger.info(iex_token)
            today = '2020-12-28'
            # today = datetime.today().strftime('%Y-%m-%d')
            data = etl.extract(today, logger)
            etl.load(data, logger)
        except:
            logger.exception('')


if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(TradingMonkey_ETL)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(TradingMonkey_ETL)

    # obj = TradingMonkey_ETL(args='')
    # obj.SvcDoRun()