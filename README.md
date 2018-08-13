# OHLCV Data Aggregator

## Overview

If you are looking for high-resolution cryptoasset trading time series data (for backtesting your trading algos or making high-quality interactive visualizations), you will soon find yourself in a tough position. 

No exchange that I know of provides high-res OHLCV data far into the past. Your best bet would be [Poloniex](https://www.poloniex.com/) API which can provide you with 5-minute period OHLCV data from several years, depending on the cryptoasset listing time with Poloniex. The past not-so-high-res (i.e. 5-minute period or higher) data time window that you can fetch ranges from 1-6 months depending on the exchange. Very few offer 1-minute period at all.

The other option is to acquire the data from a paid source. I haven't explored this avenue since I didn't like the idea of spending money on a data of questionable quality sourced from someone unaffiliated with an exchange.

The final option is to gradually aggregate the data yourself directly from exchanges. This option is the most time consuming and you won't be able to capture past data more than a few months back. But if you are in no hurry to use the data right now, you might as well start aggregating it now so that you have some good quality high-res data when you need it a year down the road. All that is needed to aggregate your own data is this tool and a machine that can run a cron job every hour or so (the cheapest EC2 instance works great for this!)

## Description

When scheduled as a cron job, `ohlcv_aggregator.py` aggregates OHLCV (open-high-low-close-volume) data from selected cryptoasset exchanges. For easier interfacing with different exchanges' APIs, [ccxt](https://github.com/ccxt/ccxt) library is used.

By examining top-50 or so cryptoasset exchanges by volume (as of February 2018), it was found that very few provide 1-min period OHLCV data. The ones that do provide such data are listed as keys in `whitelist` dict in `whitelist.py` (more on whitelisting in **Usage** section). The key to efficient aggrefgation of time series data without breaks or overlapping is to do so in batches. E.g. every hour fetch 1-minute period OHLCV data for the last hour and check that the timestamp of the first entry in the batch is exactly one minute ahead of the timestamp in the stored time series. Because exchanges' APIs are not perfect and fetching large batches from several exchanges takes time, it's much better to fetch overlapping data. For instance, fetch latest 1 hour and 15 minutes time series data every hour and make sure to remove the overlapping part from the batch before merging with existing data.

With the existing configuration, `ohlcv_aggregator.py` fetches 1-minute period data from selected exchanges as far into the past as it can (less than 24 hours of data in almost every case). The data is fetched from exchanges listed in `whitelist.py`; those exchanges have relatively high volume and make 1-minute period OHLCV data available. For some exchanges it made more sense to fetch only select pairs due to low volume on other pairs, e.g. ` 'okex': ['LTC/BTC', 'ETH/BTC', 'ETC/BTC', 'BCH/BTC', ... ]` only listed pairs are fetched from Okex exchange. For other exchanges, it was acceptable to fetch all available pairs. e.g. `'binance': []`.

All whitelisted exchanges were tested to determine how far into the past one can access 1-minute data. At the moment of launching the aggregator, Binance exchange was determined to be the limiting factor on the frequency of data polling. Since it is possible to fetch 1-minute OHLCV data only 1.5-2 hours into the past on that exhcange, it was a good idea to run `ohlcv_aggregator.py` every hour. A lot more data than from the last hour is fetched from other exchanges but the script takes care of that and makes sure that the time series are consistent and have no duplicate data.

All the data is stored in [SQLite](https://sqlite.org/index.html) database. It's a good choice since Python standard library has connectors to it and we don't really need external editing capabilities for this usecase (SQLite databases can't be accessed remotely; there's no daemon listening for connections, the database is just a single file).

## Usage

1. Decide which exchanges have OHLCV data you need in the appropriate format. When choosing, might want to consider:

- Available pairs
- Volume
- Availalbe period

2. Once you made your choice and filled `whitelist.py`, provide appropriate arguments to OHLCVAggregator instantiation at the bottom of `ohlcv_aggregator.py`. E.g:

```python
from whitelist import whitelist
database_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'ohlcv.db'))
aggregator = OHLCVAggregator(db_path=database_path, period='1m', whitelist=whitelist)
```

3. Create a cron job that will be running `ohlcv_aggregator.py`. To determine the required cron job frequency, check which exchange provides the least amount of datapoints for a chosen period. Choose cron job frequency so that you won't get any breaks in the data for that exchange.

4. Since SQLite database is not accessible remotely, the most convenient way to access the data is to run a web UI that allows viewing and downloading data; [coleifer/sqlite-web](https://github.com/coleifer/sqlite-web) is a great tool for that.

5. Set some alerts so that you can quickly react if there's any interruption in the aggregation process over time. Since I am using AWS EC2 t2.nano to run the aggregator, I set a CloudWatch alarm that will notify me when there's been less than 1000 incoming network packets in the last hour. Since the aggregator is the only service running on the computer that fetches external data periodically, this alarm will be trigerred if something is wrong with the script.

It is also possible that something on the exchange side may interfere with the proper aggregation process. Examples: exchange is down, API is down etc. These can be detected by running `check_data_consistency.py`. If there are breaks in data (i.e. adjacent 1-minute OHLCV records have timestamps delta > 60s) or duplicate records for any table in the database created by `ohlcv_aggregator.py`, they can be uncovered with that script.


