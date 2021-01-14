from datetime import datetime
import servicemanager
import socket
import sys
import win32event
import win32service
import win32serviceutil
import env
from src import util, etl, iex

logger = util.setup_logger('tradingMonkey_ETL')
interval = 15 * 60 * 1000 # 15 minutes

class TradingMonkey_ETL(win32serviceutil.ServiceFramework):
    _svc_name_ = 'TradingMonkey_ETL'
    _svc_display_name_ = 'Trading Monkey ETL'
    _svc_description_ = 'Extracts stock quotes from the IEX API and loads them to MSSQL'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        logger.info('service stopping')
        win32event.SetEvent(self.stop_event)

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
        logger.info('service starting')
        self.main()

    def main(self):
        try:
            # wait for 5 second before reporting that it's running
            obj = win32event.WaitForSingleObject(self.stop_event, 5000)
            self.ReportServiceStatus(win32service.SERVICE_RUNNING)

            isMarketOpen = False
            while obj != win32event.WAIT_OBJECT_0:
                temp = iex.is_market_open()
                if isMarketOpen or temp: # run it one more time after the market closes
                    data = etl.extract(logger)
                    etl.load(data, logger)
                else:
                    logger.info('US Market is closed')
                isMarketOpen = temp
                obj = win32event.WaitForSingleObject(self.stop_event, interval)
        except:
            logger.exception('')         

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(TradingMonkey_ETL)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(TradingMonkey_ETL)
