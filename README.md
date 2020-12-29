# TradingMonkey_ETL

Steps to setup and run the notebook

1. Create a virtual environment. (Only do this the first time)
<br>
`python3 -m venv venv`

2. Activate the virtual environement
<br>
`source venv/bin/activate`
<br>
`venv\Scripts\activate.bat`

3. Install the dependencies
<br>
`pip install -r requirements.txt`

4. Create a .env file
<br>
Create a file at the root of the project with the name ".env" and save the following to the file:
<br>
```
IEX_TOKEN=<value>
IEX_API_VERSION=iexcloud-sandbox
MSSQL_SERVER=<value>
MSSQL_DB=<value>
MSSQL_UID=<value>
MSSQL_PWD=<value>
```
5. Install the odbc driver
https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15

6. Start Jupyter notebooks and open test.ipynb once it loads in your browser.
<br>
`jupyter notebook`
