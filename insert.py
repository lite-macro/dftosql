import sqlite3
import psycopg2.extras
import pymysql
import cytoolz
import pandas as pd
from dftosql.utils import Iter, logger, cols_lite, cols_my, cols_pg, placeholders_lite, placeholders_my, placeholders_pg, quote_tb_lite, quote_tb_my, quote_tb_pg
from dftosql.utils import conn_lite, conn_pg, conn_my


@cytoolz.curry
def i_sql(table: str, cols: str, values: str) -> str:
    return 'insert into {0}({1}) values({2})'.format(table, cols, values)


@cytoolz.curry
def i_sql_lite(table: str, cols: Iter) -> str:
    return i_sql(quote_tb_lite(table), cols_lite(cols), placeholders_lite(len(cols)))


@cytoolz.curry
def i_sql_my(table: str, cols: Iter) -> str:
    return i_sql(quote_tb_my(table), cols_my(cols), placeholders_my(len(cols)))


@cytoolz.curry
def i_sql_pg(table: str, cols: Iter) -> str:
    return i_sql(quote_tb_pg(table), cols_pg(cols), placeholders_pg(len(cols)))


@cytoolz.curry
def i_lite(conn: conn_lite, table: str, df: pd.DataFrame) -> None:
    cur = conn.cursor()
    li = df.replace('', pd.NaT).where(pd.notnull(df), None).values.tolist()
    cols = list(df)
    sql = i_sql_lite(table, cols)
    for row in li:
        try:
            print(sql, tuple(row))
            cur.execute(sql, tuple(row))
        except sqlite3.IntegrityError as e:
            conn.commit()
            if 'UNIQUE constraint failed:' in str(e):
                logger.warn(e)
            else:
                logger.error(e)
                raise type(e)(e)
        except Exception as e:
            logger.error(e)
            conn.commit()
            raise type(e)(e)
    conn.commit()


@cytoolz.curry
def i_my(conn: conn_my, table: str, df: pd.DataFrame) -> None:
    cur = conn.cursor()
    li = df.replace('', pd.NaT).where(pd.notnull(df), None).values.tolist()
    cols = list(df)
    sql = i_sql_my(table, cols)
    for row in li:
        try:
            print(sql, tuple(row))
            cur.execute(sql, tuple(row))
        except sqlite3.IntegrityError as e:
            conn.commit()
            if 'UNIQUE constraint failed:' in str(e):
                logger.warn(e)
            else:
                logger.error(e)
                raise type(e)(e)
        except Exception as e:
            logger.error(e)
            conn.commit()
            raise type(e)(e)
    conn.commit()

    
@cytoolz.curry
def i_pg(conn: conn_pg, table: str, df: pd.DataFrame) -> None:
    cur = conn.cursor()
    li = df.replace('', pd.NaT).where(pd.notnull(df), None).values.tolist()
    cols = list(df)
    sql = i_sql_pg(table, cols).replace('(%)', '(%%)') ## replace '%' with '%%' because '%' is special character in postgresql
    for row in li:
        try:
            print(sql, tuple(row))
            cur.execute(sql, tuple(row))
            conn.commit()
        except sqlite3.IntegrityError as e:
            conn.commit()   # the same as conn.rollback() because pgsql rollback entire transaction whenever error occur
            if 'UNIQUE constraint failed:' in str(e):
                logger.warn(e)
            else:
                logger.error(e)
                raise type(e)(e)
        except Exception as e:
            logger.error(e)
            conn.commit()   # the same as conn.rollback() because pgsql rollback entire transaction whenever error occur
            raise type(e)(e)
    conn.commit()

@cytoolz.curry
def i_pg_batch(conn: conn_pg, table: str, df: pd.DataFrame) -> None:
    cur = conn.cursor()
    li = df.replace('', pd.NaT).where(pd.notnull(df), None).values.tolist()
    cols = list(df)
    sql = i_sql_pg(table, cols).replace('(%)', '(%%)') ## replace '%' with '%%' because '%' is special character in postgresql
    try:
        print(sql)
        psycopg2.extras.execute_batch(cur, sql, li)
        conn.commit()
    except psycopg2.IntegrityError as e:
        conn.commit()   # the same as conn.rollback() because pgsql rollback entire transaction whenever error occur
        if 'UNIQUE constraint failed:' in str(e):
            logger.warn(e)
        else:
            logger.error(e)
            raise type(e)(e)
    except Exception as e:
        logger.error(e)
        conn.commit()   # the same as conn.rollback() because pgsql rollback entire transaction whenever error occur
        raise type(e)(e)
    conn.commit()

