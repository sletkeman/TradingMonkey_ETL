"""
    Defines a windows service to refresh the historical data every 5 minutes
"""

import socket
import sys
import servicemanager
import win32event
import win32service
import win32serviceutil
import env
from lib.util import setup_logger
from logic.history_refresh import extract, load
from lib.db import reset_flag

logger = setup_logger('tradingMonkey_ETL')
INTERVAL = 5 * 60 * 1000 # 15 minutes

class TradingMonkey(win32serviceutil.ServiceFramework):
    _svc_name_ = 'TradingMonkey_HistoryRefresh'
    _svc_display_name_ = 'Trading Monkey History Refresh'
    _svc_description_ = 'Refreshes the historical quote data'

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

            while obj != win32event.WAIT_OBJECT_0:
                data = extract(logger)
                if len(data):
                    load(data)
                    reset_flag(data['Symbol'].tolist())
                obj = win32event.WaitForSingleObject(self.stop_event, INTERVAL)
        except:
            logger.exception('')

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(TradingMonkey)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(TradingMonkey)
