import psycopg2
from config import EOD_DB_HOST, EOD_DB_NAME, EOD_DB_USER, EOD_DB_PASS, EOD_DB_PORT

def get_db_connection():
    return psycopg2.connect(
        host=EOD_DB_HOST,
        database=EOD_DB_NAME,
        user=EOD_DB_USER,
        password=EOD_DB_PASS,
        port=EOD_DB_PORT
    )

def close_db_connection(conn, cur=None):
    if cur is not None:
        cur.close()
    conn.close()


def execute_query(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()

def fetch_raw_data(conn, symbol, days, eod_date):
    query = """SELECT date, symbol, close, aclose
               FROM ohlc
               WHERE Symbol = %s AND date < %s
               ORDER BY Date DESC LIMIT %s"""
    return execute_query(conn, query, (symbol, eod_date, days))

def fetch_selected_symbols_data(conn, symbols, start_date, end_date):
    query = """SELECT date, symbol, close, aclose
               FROM ohlc
               WHERE symbol = ANY(%s) AND date BETWEEN %s AND %s"""
    return execute_query(conn, query, (symbols, start_date, end_date))

def fetch_all_symbols_that_traded_last_number_of_days(conn, last_date, number_of_days):
    query = """SELECT DISTINCT symbol
               FROM ohlc
               WHERE date BETWEEN (%s::date - INTERVAL '%s days') AND %s::date"""
    return execute_query(conn, query, (last_date, number_of_days, last_date))
