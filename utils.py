
import sqlite3
import psycopg2
import pymysql
import cytoolz.curried
import pandas as pd
from typing import Iterable, List
import logging

# 產生新的logger
logger = logging.Logger('test')

# 定義 handler 輸出 sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# 設定輸出格式
formatter = logging.Formatter('%(asctime)s: %(levelname)-s %(module)s %(lineno)d  %(funcName)s %(message)s')
# handler 設定輸出格式
console.setFormatter(formatter)

logger.addHandler(console)

pd.get_option("display.max_rows")
pd.get_option("display.max_columns")
pd.set_option("display.max_rows", 100)
pd.set_option("display.max_columns", 1000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.unicode.east_asian_width', True)

conn_lite = sqlite3.Connection
conn_pg = psycopg2.extensions.connection
conn_my = pymysql.connections.Connection
Iter = Iterable
error = str

@cytoolz.curry
def join(s: str, it: Iter[str]) -> str:
    return s.join(it)


@cytoolz.curry
def placeholders(placeholder: str, length) -> str:
    return ', '.join([placeholder]*length)


def placeholders_lite(length) -> str:
    return placeholders('?', length)


def placeholders_my(length) -> str:
    return placeholders('%s', length)


def placeholders_pg(length) -> str:
    return placeholders('%s', length)


@cytoolz.curry
def quote(symbol: any, it: Iter) -> List[str]:
    return ['{0}{1}{0}'.format(symbol, col) for col in it]


@cytoolz.curry
def quote_join(symbol_quote: any, it: Iter) -> str:
    return cytoolz.compose(join(', '), quote(symbol_quote))(it)


@cytoolz.curry
def quote_tb(symbol: any, table: str) -> str:
    return '{0}{1}{0}'.format(symbol, table)


def quote_tb_lite(table: str) -> str:
    return quote_tb('`', table)


def quote_tb_my(table: str) -> str:
    return quote_tb('`', table)


def quote_tb_pg(table: str) -> str:
    return quote_tb('"', table)


def cols_lite(it: Iter) -> str:
    return quote_join('`', it)


def cols_my(it: Iter) -> str:
    return quote_join('`', it)


def cols_pg(it: Iter) -> str:
    return quote_join('"', it)