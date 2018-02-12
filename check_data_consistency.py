#!/usr/bin/env python

import os
from ohlcv_aggregator import OHLCVAggregator
from whitelist import whitelist

def get_all_unix_ms(cursor, table_name):
    """Gets the latest recorded unix_ms timestamp from specified table."""
    try:
        sql = "SELECT unix_close FROM `{}` ORDER BY unix_close ASC".format(table_name)
        cursor.execute(sql)
        result = cursor.fetchall()
        all_unix = [x[0] for x in result]
        return all_unix
    except Exception as e:
        print(str(e))

def check_period(all_unix_ms, period_ms, table_name):
    """Checks if adjacent OHLCV records in a table have expected period."""
    prev_unix_ms = all_unix_ms[0] - period_ms
    for unix_ms in all_unix_ms:
        if unix_ms - prev_unix_ms != period_ms:
            print('Inconsistency in {}. At least two adjacent OHLCV records have period of {} ms'.format(table_name, unix_ms-prev_unix_ms))
        prev_unix_ms = unix_ms

def check_data_consistency(cursor, table_names, period_ms):
    """Checks every table in db for unix_ms period consistency between adjacent OHLCV records."""
    for table_name in table_names:
        all_unix_ms = get_all_unix_ms(cursor, table_name)
        check_period(all_unix_ms, period_ms, table_name)

def main():
    wl = whitelist
    database_path = os.path.join(os.getcwd(), 'data', 'ohlcv.db')
    aggregator = OHLCVAggregator(db_path=database_path, period='1m', whitelist=wl)
    check_data_consistency(aggregator.cursor, aggregator.table_names, period_ms=60000)

if __name__ == '__main__':
    main()
