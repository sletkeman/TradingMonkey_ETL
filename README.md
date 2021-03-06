# TradingMonkey_ETL

Steps to setup and run the notebook

1. Create a virtual environment. (Only do this the first time)
<br>
`python -m venv venv`

2. Activate the virtual environement
<br>
`source venv/bin/activate`
<br>
`venv\Scripts\activate.bat`

3. Install the dependencies
<br>
`pip install -r src\requirements.txt`

4. Environment variables
<br>
For notebooks, create a file at the root of the project with the name ".env" and save the following to the file:
<br>
```
IEX_TOKEN=<value>
IEX_API_VERSION=iexcloud-sandbox
MSSQL_SERVER=<value>
MSSQL_DB=<value>
MSSQL_UID=<value>
MSSQL_PWD=<value>
```

for the actual service, create env.py in the src directory
```
from os import environ
environ['IEX_TOKEN'] = ''
environ['IEX_API_VERSION'] = 'iexcloud-sandbox'
environ['MSSQL_SERVER'] = ''
environ['MSSQL_DB'] = ''
environ['MSSQL_UID'] = ''
environ['MSSQL_PWD'] = ''
```

5. Install the odbc driver
https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15

6. Start Jupyter notebooks and open test.ipynb once it loads in your browser.
<br>
`jupyter notebook`

7. Build the service
<br>
`pyinstaller -p src --hidden-import=win32timezone src\services\quotes.py`
<br>
`pyinstaller -p src --hidden-import=win32timezone src\services\history.py`

7. Install or update the service. Note that it must be run from an admin console window with the venv activated
<br>
`dist\quotes\quotes.exe install`
<br>
`dist\history\history.exe install`
