#!/usr/bin/env python

import time
import os
import json
import sqlite3
import requests
import logging
import ccxt
from whitelist import whitelist

logger = logging.Logger('catch_all')

class OHLCVAggregator(object):

    """Creates a db containing separate tables with OHLCV data for select pairs using ccxt lib.

    To aggregate OHLC data from cryptowat.ch with no breaks, it's sufficient to initialize
    this class and periodically call update_ohlc() method. The period with which the method
    should be called depends on how often you want to update the db with the latest info
    and the period of the OHLC data that you are connecting. Elaborating on the latter:
    it looks like the cryptowat.ch OHLC API offers a max of 499 OHLC records at a time;
    this means that in order to collect 60 sec OHLC data with no breaks, you need to
    call update_ohlc() every 499 * 60 sec = 29940 sec ~= 8.3 hours. More often is
    better since it takes some time to fetch OHLC data for all 500+ pairs.

    Attributes:
        db_path: A path that contains/will contain SQLite3 db.
        connection: SQLite3 db connection; used for commiting INSERT transactions.
        cursor: SQLite db cursor; used for executing all SQL queries.
        period: An string that specifies desired OHLCV period.
        whitelist: A dict containing exchanges and pairs that we want to record.
        track_pairs: A set of all asset pairs that the aggregator is tracking.
                    Corresponds to table names used in the db.
    """

    HTTP_OK = 200

    def __init__(self, db_path, period, whitelist):
        """Inits CryptowatchOHLCAggregator with period and db_path."""
        self.db_path = db_path
        self.connection, self.cursor = self.__init_db()
        self.period = period
        self.whitelist = whitelist
        self.track_pairs = []

    def __init_db(self):
        """Inits db connection and cursor."""
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        return connection, cursor

    def __init_track_pairs(self):
        """ Get existing tracked pairs by examining table names in the db."""
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        result = self.cursor.fetchall() # gets all existing table names (asset pairs)
        track_pairs = set([x[0] for x in result])
        return track_pairs

    def __create_table(self, table_name):
        """ Creates new table to hold OHLC data."""
        try:
            self.cursor.execute("""CREATE TABLE IF NOT EXISTS {} (unix_close INT PRIMARY KEY,
                open REAL, high REAL, low REAL, close REAL, volume REAL)""".format(table_name))
        except Exception as e:
            print(str(e))

    def __get_latest_unix_ms(self, table_name):
        """Gets the latest recorded unix_ms timestamp from specified table."""
        try:
            sql = "SELECT unix_close FROM `{}` ORDER BY unix_close DESC LIMIT 1".format(table_name)
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            if result != None:
                return result[0]
        except Exception as e:
            print(str(e))

    @staticmethod
    def __add_ohlcv_row_sql(table_name, ohlcv_row):
        """ Add a single OHLC record to a selected table."""
        unix_ms, open_, high, low, close, *volume = ohlcv_row
        if len(volume) == 2: # in case base and quote volumes are given, disregard base
            volume, _ = volume
        else:
            volume = volume[0]
        sql_query = """INSERT INTO `{}` (unix_close, open, high, low, close, volume)
            VALUES (?,?,?,?,?,?);""".format(table_name)
        sql_args = (unix_ms, open_, high, low, close, volume)
        return (sql_query, sql_args)

    def __insert_tx(self, table_name, ohlcv_data):
        """ Insert all unseen OHLC records to a table in one transaction."""
        latest_unix_ms = self.__get_latest_unix_ms(table_name)
        if not latest_unix_ms:
            latest_unix_ms = 0 # case when table is empty
        sql_tx = []
        for ohlcv_row in ohlcv_data:
            unix_ms = ohlcv_row[0]
            if unix_ms > latest_unix_ms:
                sql = OHLCVAggregator.__add_ohlcv_row_sql(table_name, ohlcv_row)
                sql_tx.append(sql)
        self.cursor.execute('BEGIN TRANSACTION')
        for sql_query, sql_args in sql_tx:
            try:
                self.cursor.execute(sql_query, sql_args)
            except:
                pass
        self.connection.commit()
        
    def update_ohlc(self):
        """Updates the db with the latest OHLC data from cryptowat.ch"""
        exchange_names = self.whitelist['exchanges'].keys()
        for exchange_name in exchange_names:
            exchange = getattr(ccxt, exchange_name)()
            markets = exchange.load_markets()
            pairs = self.whitelist['exchanges'][exchange_name]
            if len(pairs) == 0:
                pairs = markets.keys()
            for pair in pairs:
                try:
                    # try fetching ohlcv data... proceed with other stuff only if this succeeds
                    ohlcv_data = exchange.fetch_ohlcv(pair, self.period)
                    exchange_name = exchange_name.replace('-', '_') # replace hyphens for SQL
                    pair = pair.replace('/', '_') # replace fslash for SQL
                    table_name = '{}_{}'.format(pair, exchange_name)
                    if table_name not in self.track_pairs:
                        self.__create_table(table_name) # create table for unseen pair if needed
                        self.track_pairs.append(table_name)
                    self.__insert_tx(table_name, ohlcv_data)
                except ccxt.errors.NotSupported as e:
                    print(logger.error(e, exc_info=True))

                except ccxt.errors.AuthenticationError as e:
                    print(logger.error(e))
                except ccxt.errors.DDoSProtection as e:
                    print(logger.error(e))
                    print('DDOS ERROR')
                except ccxt.errors.RequestTimeout as e:
                    print(logger.error(e))
                    print('TIMEOUT ERROR')
                except ccxt.errors.ExchangeNotAvailable as e:
                    print(logger.error(e))
                    print('EXCHANGE NOT AVAILABLE')
                except ccxt.errors.ExchangeError as e:
                    print(logger.error(e))

def main():
    wl = whitelist
    database_path = os.path.join(os.getcwd(), 'data', 'ohlcv.db')
    aggregator = OHLCVAggregator(db_path=database_path, period='1m', whitelist=wl)
    aggregator.update_ohlc()

if __name__ == '__main__':
    main()
