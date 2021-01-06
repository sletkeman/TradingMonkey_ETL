import servicemanager
import socket
import sys
import win32event
import win32service
import win32serviceutil
from datetime import datetime
import env
from src import util
import etl

logger = util.setup_logger('tradingMonkey_ETL')

class TradingMonkey_ETL(win32serviceutil.ServiceFramework):
    _svc_name_ = 'TradingMonkey_ETL'
    _svc_display_name_ = 'Trading Monkey ETL'
    _svc_description_ = 'Extracts stock quotes from the IEX API and loads them to MSSQL'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        # socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)

    def SvcDoRun(self):
        logger.info('Service Starting')
        try:
            today = '2020-12-28'
            # today = datetime.today().strftime('%Y-%m-%d')
            data = etl.extract(today, logger)
            etl.load(data, logger)
        except:
            logger.exception('')
        finally:
            logger.info('Service Stopping')


if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(TradingMonkey_ETL)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(TradingMonkey_ETL)

    # obj = TradingMonkey_ETL(args='')
    # obj.SvcDoRun()