from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from uuid import uuid4 #Generación de UUID
from uuid import UUID #Convertir cadenas a UUID
import datetime

'''
Esta aplicación tiene como proposito el entender un poco más la funcionalidad de cassandra en un script de python 
y aprender sobre la automatización
'''

# Conexión al cluster de Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Creación del keyspace 'investment_portfolio'
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS investment_portfolio
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
""")

# Selección del keyspace
session.set_keyspace('investment_portfolio')

# Creación de tablas
session.execute("""
CREATE TABLE IF NOT EXISTS users (
    username text PRIMARY KEY,
    name text
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS accounts (
    account_num uuid PRIMARY KEY,
    username text,
    cash_balance decimal,
    investment_value decimal,
    total_value decimal
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS trades (
    trade_id uuid PRIMARY KEY,
    account_num uuid,
    type text,
    symbol text,
    shares int,
    price decimal,
    amount decimal,
    date timestamp
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS instruments (
    symbol text PRIMARY KEY,
    quote decimal,
    current_value decimal,
    quantity int
)
""")

# Inserción de datos ejemplo
# Usuario
username1 = 'user1'
session.execute(
    """
    INSERT INTO users (username, name) VALUES (%s, %s)
    """,
    (username1, 'Francisco Flores')
)

username2 = 'user2'
session.execute(
    """
    INSERT INTO users (username, name) VALUES (%s, %s)
    """,
    (username2, 'Enrique Flores')
)

# Cuenta
account_num1 = uuid4()
session.execute(
    """
    INSERT INTO accounts (account_num, username, cash_balance, investment_value, total_value) VALUES (%s, %s, 10000, 5000, 15000)
    """,
    (account_num1, username1)
)

account_num2 = uuid4()
session.execute(
    """
    INSERT INTO accounts (account_num, username, cash_balance, investment_value, total_value) VALUES (%s, %s, 10000, 5000, 15000)
    """,
    (account_num2, username2)
)

# Trades
trade1_id = uuid4()
session.execute(
    """
    INSERT INTO trades (trade_id, account_num, type, symbol, shares, price, amount, date) VALUES (%s, %s, 'buy', 'AAPL', 10, 150.0, 1500.0, %s)
    """,
    (trade1_id, account_num1, datetime.datetime.now())
)

trade2_id = uuid4()
session.execute(
    """
    INSERT INTO trades (trade_id, account_num, type, symbol, shares, price, amount, date) VALUES (%s, %s, 'sell', 'MSFT', 5, 220.0, 1100.0, %s)
    """,
    (trade2_id, account_num1, datetime.datetime.now())
)

# Instrumentos
session.execute(
    """
    INSERT INTO instruments (symbol, quote, current_value, quantity) VALUES ('AAPL', 150.0, 1500.0, 10)
    """
)

session.execute(
    """
    INSERT INTO instruments (symbol, quote, current_value, quantity) VALUES ('MSFT', 220.0, 1100.0, 5)
    """
)

#Consultas
def query_accounts_by_username(username):
    query = "SELECT * FROM accounts WHERE username = %s ALLOW FILTERING;"
    rows = session.execute(query, (username,))
    for row in rows:
        print(row)

def query_balance_by_account_uuid(account_uuid):
    query = "SELECT cash_balance, investment_value, total_value FROM accounts WHERE account_num = %s;"
    rows = session.execute(query, (UUID(account_uuid),))  # Conversión de string a UUID
    for row in rows:
        print(row)

def query_trades_by_date_range(account_uuid, start_date, end_date):
    query = """
    SELECT * FROM trades WHERE account_num = %s
    AND date >= %s AND date <= %s ALLOW FILTERING;
    """
    rows = session.execute(query, (UUID(account_uuid), start_date, end_date))
    for row in rows:
        print(row)

def query_type_trades_by_date_range(account_uuid, start_date, end_date):
    query = """
    SELECT * FROM trades WHERE account_num = %s
    AND date >= %s AND date <= %s AND type = 'buy' ALLOW FILTERING;
    """
    rows = session.execute(query, (UUID(account_uuid), start_date, end_date))
    for row in rows:
        print(row)

def query_type_instrument_trades_by_date_range(account_uuid, start_date, end_date):
    query = """
    SELECT * FROM trades WHERE account_num = %s
    AND date >= %s AND date <= %s AND type = 'buy' AND symbol = 'AMZN' 
    ALLOW FILTERING;
    """
    rows = session.execute(query, (UUID(account_uuid), start_date, end_date))
    for row in rows:
        print(row)

def query_instrument_trades_by_date_range(account_uuid, start_date, end_date):
    query = """
    SELECT * FROM trades WHERE account_num = %s
    AND date >= %s AND date <= %s AND symbol = 'AMZN' ALLOW FILTERING;
    """
    rows = session.execute(query, (UUID(account_uuid), start_date, end_date))
    for row in rows:
        print(row)

account_uuid = 'dfe4017b-b7fc-4fea-be76-7c2144207fbf'  
start_date = '2024-02-28 04:27:30.140000'
end_date = '2024-02-29 01:00:47.139000'

# Conversión de string a datetime
start_date_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S.%f')
end_date_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S.%f')

# Ejecución las consultas
query_accounts_by_username('user2')
query_balance_by_account_uuid(account_uuid)
query_trades_by_date_range(account_uuid, start_date_dt, end_date_dt)
query_type_trades_by_date_range(account_uuid, start_date_dt, end_date_dt)
query_type_instrument_trades_by_date_range(account_uuid, start_date_dt, end_date_dt)
query_instrument_trades_by_date_range(account_uuid, start_date_dt, end_date_dt)

# Cerrar la conexión
cluster.shutdown()

print("Termino correctamente")