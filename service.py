from datetime import datetime
import servicemanager
import socket
import sys
import win32event
import win32service
import win32serviceutil
import env
from src import util, etl

logger = util.setup_logger('tradingMonkey_ETL')
interval = 15 * 60 * 1000 # 15 minutes

class TradingMonkey_ETL(win32serviceutil.ServiceFramework):
    _svc_name_ = 'TradingMonkey_ETL'
    _svc_display_name_ = 'Trading Monkey ETL'
    _svc_description_ = 'Extracts stock quotes from the IEX API and loads them to MSSQL'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.waitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.waitStop)

    def SvcDoRun(self):
        logger.info('service started')
        obj = None
        try:
            while obj != win32event.WAIT_OBJECT_0:
                today = datetime.today().strftime('%Y-%m-%d')
                data = etl.extract(today, logger)
                etl.load(data, logger)
                obj = win32event.WaitForSingleObject(self.waitStop, interval)
        except:
            logger.exception('')
        finally:
            logger.info('service stopping')

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(TradingMonkey_ETL)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(TradingMonkey_ETL)
