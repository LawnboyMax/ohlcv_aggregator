#!/usr/bin/env python

import os
import sqlite3
import logging
import ccxt
from whitelist import whitelist

logger = logging.Logger('catch_all')

class OHLCVAggregator(object):

    """Creates a db containing separate tables with OHLCV data for select pairs using ccxt lib.

    To aggregate OHLCV data using ccxt library with no breaks, it's sufficient to initialize
    this class and periodically call update_ohlcv() method. The period with which the method
    should be called depends on how often you want to update the db with the latest info
    and the period of the OHLCV data that you are fetching. Elaborating on the latter:
    it looks like the some exchanges' API offers a very small number of OHLCV records at a time
    (e.g. Hitbtc offers 100 at a time); this means that in order to collect 60 sec OHLCV data
    with no breaks, you to call update_ohlcv() every 100 * 60 sec = sec ~= 1.6 hours
    (just set up an hourly cron job).

    Attributes:
        db_path: A path that contains/will contain SQLite3 db.
        connection: SQLite3 db connection; used for commiting INSERT transactions.
        cursor: SQLite db cursor; used for executing all SQL queries.
        period: An string that specifies desired OHLCV period.
        whitelist: A dict containing exchanges and their pairs that we want to fetch.
        track_pairs: A set of all asset pairs that the aggregator is tracking.
                    Corresponds to table names used in the db.
    """

    def __init__(self, db_path, period, whitelist):
        """Inits CryptowatchOHLCAggregator with period and db_path."""
        self.db_path = db_path
        self.connection, self.cursor = self.__init_db()
        self.period = period
        self.whitelist = whitelist
        self.track_pairs = self.__init_track_pairs()

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

    @staticmethod
    def __create_table_name(exchange_name, pair_name):
        """Create a table name to be inserted to the db"""
        exchange_name = exchange_name.replace('-', '_') # replace hyphens for SQL, if present
        pair_name = pair_name.replace('/', '_') # replace fslash for SQL (BTC/ETH -> BTC_ETH)
        table_name = '{}_{}'.format(pair_name, exchange_name)
        return table_name

    def __create_table(self, table_name):
        """ Creates new table to hold OHLC data."""
        try:
            self.cursor.execute("""CREATE TABLE IF NOT EXISTS `{}` (unix_ms_close INT PRIMARY KEY,
                open REAL, high REAL, low REAL, close REAL, volume REAL)""".format(table_name))
        except Exception as e:
            print(str(e))

    def __get_latest_unix_ms(self, table_name):
        """Gets the latest recorded unix_ms timestamp from specified table."""
        try:
            sql = "SELECT unix_ms_close FROM `{}` ORDER BY unix_ms_close DESC LIMIT 1".format(table_name)
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
        # in case base and quote volumes are given, disregard base
        if len(volume) == 2:
            volume, _ = volume
        else:
            volume = volume[0]
        sql_query = """INSERT INTO `{}` (unix_ms_close, open, high, low, close, volume)
            VALUES (?,?,?,?,?,?);""".format(table_name)
        sql_args = (unix_ms, open_, high, low, close, volume)
        return (sql_query, sql_args)

    def __insert_tx(self, table_name, ohlcv_data):
        """ Insert all unseen OHLC records to a table in one transaction."""
        latest_unix_ms = self.__get_latest_unix_ms(table_name)
        # case when table is empty
        if not latest_unix_ms:
            latest_unix_ms = 0
        sql_tx = []
        for ohlcv_row in ohlcv_data:
            unix_ms = ohlcv_row[0]
            # add fresh data only
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

    def update_ohlcv(self):
        """Updates the db with the latest OHLC data from cryptowat.ch"""
        exchange_names = self.whitelist['exchanges'].keys()
        for exchange_name in exchange_names:
            exchange = getattr(ccxt, exchange_name)()
            markets = exchange.load_markets()
            pairs = self.whitelist['exchanges'][exchange_name]
            # if no particular pairs are whitelisted, fetch all pairs from market
            if not pairs:
                pairs = markets.keys()
            for pair in pairs:
                try:
                    # try fetching ohlcv data... proceed with other stuff only if this succeeds
                    ohlcv_data = exchange.fetch_ohlcv(pair, self.period)
                    table_name = OHLCVAggregator.__create_table_name(exchange_name, pair)
                    if table_name not in self.track_pairs:
                        self.__create_table(table_name) # create table for unseen pair if needed
                        self.track_pairs.add(table_name)
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
    aggregator.update_ohlcv()

if __name__ == '__main__':
    main()
