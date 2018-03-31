#!/usr/bin/env python

import os
import sqlite3
import logging
import ccxt
from whitelist import whitelist


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
        table_names: A set of all asset pairs that the aggregator is tracking.
                    Corresponds to table names used in the db.
    """

    def __init__(self, db_path, period, whitelist):
        """Inits CCXT_OHLC_Aggregator with period and db_path."""
        logging.basicConfig(
            filename=os.path.abspath(os.path.join(os.path.dirname(__file__), 'logs', 'hello.log')),
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(name)s %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.requests_log = logging.getLogger("requests").setLevel(logging.INFO)
        self.db_path = db_path
        self.connection, self.cursor = self.__init_db()
        self.period = period
        self.whitelist = whitelist
        self.table_names = self.__init_table_names()

    def __init_db(self):
        """Inits db connection and cursor."""
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        return connection, cursor

    def __init_table_names(self):
        """ Get existing table names from the db."""
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        result = self.cursor.fetchall() # gets all existing table names (asset pairs)
        table_names = set([x[0] for x in result])
        return table_names

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
        """Updates the db with the latest OHLC data from whitelisted exchanges"""
        exchange_names = self.whitelist['exchanges'].keys()
        for exchange_name in exchange_names:
            exchange = getattr(ccxt, exchange_name)()
            try:
                markets = exchange.load_markets()
            except Exception as e:
                self.logger.error(e, exc_info = True)
                continue # don't proceed if loading market isn't working
            pairs = self.whitelist['exchanges'][exchange_name]
            # if no particular pairs are whitelisted, fetch all pairs from market
            if not pairs:
                pairs = markets.keys()
            for pair in pairs:
                try:
                    # try fetching ohlcv data... proceed with other stuff only if this succeeds
                    ohlcv_data = exchange.fetch_ohlcv(pair, self.period)
                    table_name = OHLCVAggregator.__create_table_name(exchange_name, pair)
                    if table_name not in self.table_names:
                        self.__create_table(table_name) # create table for unseen pair if needed
                        self.table_names.add(table_name)
                    self.__insert_tx(table_name, ohlcv_data)
                except ccxt.errors.NotSupported as e:
                    self.logger.info(e, exc_info = True)
                except ccxt.errors.AuthenticationError as e:
                    self.logger.info(e, exc_info = True)
                except ccxt.errors.DDoSProtection as e:
                    self.logger.error(e, exc_info = True)
                except ccxt.errors.RequestTimeout as e:
                    self.logger.error(e, exc_info = True)
                except ccxt.errors.ExchangeNotAvailable as e:
                    self.logger.error(e, exc_info = True)
                except ccxt.errors.ExchangeError as e:
                    self.logger.error(e, exc_info = True)

def main():
    wl = whitelist
    database_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'ohlcv.db'))
    aggregator = OHLCVAggregator(db_path=database_path, period='1m', whitelist=wl)
    aggregator.logger.info('Started reading database')
    aggregator.update_ohlcv()
    aggregator.logger.info('Finished updating records\n\n###############################################################\n')


if __name__ == '__main__':
    main()
