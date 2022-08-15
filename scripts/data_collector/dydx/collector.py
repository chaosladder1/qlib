import abc
import sys
import datetime
from abc import ABC
from pathlib import Path

import fire
import requests
import pandas as pd
from loguru import logger
from dateutil.tz import tzlocal

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))
from data_collector.base import BaseCollector, BaseNormalize, BaseRun
from data_collector.utils import deco_retry


from time import mktime
from datetime import datetime as dt
import time
from dydx3 import Client
from web3 import Web3
import pandas as pd
from datetime import timedelta
#


def get_date_set(timeinterval=1000):
  """get list of dates to download a complete dataset
    Returns
    -------
        timeinterval: the number of days of ticker data to download
    """

    periods = int(timeinterval/100)
    dates = []
    for i in range(periods):
      if i == 0:
      dates.append(datetime.today().date())
    new_date = datetime.today().date() - timedelta(days=100)

    else:
      new_date = new_date - timedelta(days=100)

    dates.append(new_date)

    return dates



def get_active_dydx():
  """download active
    Returns
    -------
        crypto symbols in given exchanges list of coingecko
    """

    client = Client(
      host='https://api.dydx.exchange'
    )

    try:
      markets = client.public.get_markets()
    market_df = pd.DataFrame(markets.data['markets']).transpose()
    market_list = market_df[market_df['status']=='ONLINE']['market'].tolist()

    except Exception as e:
      logger.warning(f"request error: {e}")
    raise
    return market_list



class CryptoCollector(BaseCollector):
  def __init__(
    self,
    save_dir: [str, Path],
    start=None,
    end=None,
    timeinterval=5000,
    interval="1d",
    max_workers=1,
    max_collector_count=2,
    delay=1,  # delay need to be one
    check_data_length: int = None,
    limit_nums: int = None,
  ):
  """
        Parameters
        ----------
        save_dir: str
            crypto save dir
        max_workers: int
            workers, default 4
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0
        interval: str
            freq, value from [1min, 1d], default 1min
        start: str
            start datetime, default None
        end: str
            end datetime, default None
        check_data_length: int
            check data length, if not None and greater than 0, each symbol will be considered complete if its data length is greater than or equal to this value, otherwise it will be fetched again, the maximum number of fetches being (max_collector_count). By default None.
        limit_nums: int
            using for debug, by default None
        """
super(CryptoCollector, self).__init__(
  save_dir=save_dir,
  start=start,
  end=end,
  interval=interval,
  max_workers=max_workers,
  max_collector_count=max_collector_count,
  delay=delay,
  check_data_length=check_data_length,
  limit_nums=limit_nums,
)

self.init_datetime()

def init_datetime(self):
    if self.interval == self.INTERVAL_1min:
    self.start_datetime = max(self.start_datetime, self.DEFAULT_START_DATETIME_1MIN)
  elif self.interval == self.INTERVAL_1d:
    pass
  else:
    raise ValueError(f"interval error: {self.interval}")

  self.start_datetime = self.convert_datetime(self.start_datetime, self._timezone)
  self.end_datetime = self.convert_datetime(self.end_datetime, self._timezone)

@staticmethod
def convert_datetime(dt: [pd.Timestamp, datetime.date, str], timezone):
  try:
  dt = pd.Timestamp(dt, tz=timezone).timestamp()
  dt = pd.Timestamp(dt, tz=tzlocal(), unit="s")
  except ValueError as e:
    pass
  return dt

@property
@abc.abstractmethod
def _timezone(self):
  raise NotImplementedError("rewrite get_timezone")



@staticmethod
def get_data_from_remote(symbol, timeinterval):
  error_msg = f"{symbol}-{timeinterval}"
try:
  dates = self.get_date_set(timeinterval=timeinterval)

for i in range(len(dates)):
  candles = client.public.get_candles(
    market=market_list[j],
    resolution='1DAY',
    to_iso = dates[i]
  ).data

_resp = pd.DataFrame(candles["candles"])
_resp['date'] = pd.to_datetime(candles_df['startedAt']).dt.date
_resp['volume'] = _resp['baseTokenVolume']

except Exception as e:
  logger.warning(f"{error_msg}:{e}")




def get_data(
  self, symbol: str, timeinterval: int
) -> [pd.DataFrame]:
  try:
  return self.get_data_from_remote(
    symbol,
    timeinterval=

  )
else:
  raise ValueError('error retrieving data.')



class CryptoCollector1d(CryptoCollector, ABC):
  def get_instrument_list(self):
    logger.info("get coingecko crypto symbols......")
    symbols = get_active_dydx()
    logger.info(f"get {len(symbols)} symbols.")
    return symbols

def normalize_symbol(self, symbol):
  return symbol

@property
def _timezone(self):
  return "UTC"


class CryptoNormalize(BaseNormalize):
  DAILY_FORMAT = "%Y-%m-%d"

@staticmethod
def normalize_crypto(
  df: pd.DataFrame,
  calendar_list: list = None,
  date_field_name: str = "date",
  symbol_field_name: str = "market",
):
  if df.empty:
    return df
  df = df.copy()
  df.set_index(date_field_name, inplace=True)
  df.index = pd.to_datetime(df.index)
  df = df[~df.index.duplicated(keep="first")]
  if calendar_list is not None:
    df = df.reindex(
      pd.DataFrame(index=calendar_list)
      .loc[
        pd.Timestamp(df.index.min()).date() : pd.Timestamp(df.index.max()).date()
        + pd.Timedelta(hours=23, minutes=59)
      ]
      .index
    )
  df.sort_index(inplace=True)

  df.index.names = [date_field_name]
  return df.reset_index()

def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
  df = self.normalize_crypto(df, self._calendar_list, self._date_field_name, self._symbol_field_name)
  return df


class CryptoNormalize1d(CryptoNormalize):
  def _get_calendar_list(self):
  return None


class Run(BaseRun):
  def __init__(self, source_dir=None, normalize_dir=None, max_workers=1, interval="1d"):
  """
        Parameters
        ----------
        source_dir: str
            The directory where the raw data collected from the Internet is saved, default "Path(__file__).parent/source"
        normalize_dir: str
            Directory for normalize data, default "Path(__file__).parent/normalize"
        max_workers: int
            Concurrent number, default is 1
        interval: str
            freq, value from [1min, 1d], default 1d
        """
super().__init__(source_dir, normalize_dir, max_workers, interval)

@property
def collector_class_name(self):
  return f"CryptoCollector{self.interval}"

@property
def normalize_class_name(self):
  return f"CryptoNormalize{self.interval}"

@property
def default_base_dir(self) -> [Path, str]:
  return CUR_DIR

def download_data(
  self,
  max_collector_count=2,
  delay=0,
  timeinterval=1000,
  start=None,
  end=None,
  check_data_length: int = None,
  limit_nums=None,
):
  """download data from Internet
        Parameters
        ----------
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0
        interval: str
            freq, value from [1min, 1d], default 1d, currently only supprot 1d
        start: str
            start datetime, default "2000-01-01"
        end: str
            end datetime, default ``pd.Timestamp(datetime.datetime.now() + pd.Timedelta(days=1))``
        check_data_length: int # if this param useful?
            check data length, if not None and greater than 0, each symbol will be considered complete if its data length is greater than or equal to this value, otherwise it will be fetched again, the maximum number of fetches being (max_collector_count). By default None.
        limit_nums: int
            using for debug, by default None
        Examples
        ---------
            # get daily data
            $ python collector.py download_data --source_dir ~/.qlib/crypto_data/source/1d --start 2015-01-01 --end 2021-11-30 --delay 1 --interval 1d
        """

super(Run, self).download_data(max_collector_count, delay, start, end, check_data_length, limit_nums)

def normalize_data(self, date_field_name: str = "date", symbol_field_name: str = "market"):
  """normalize data
        Parameters
        ----------
        date_field_name: str
            date field name, default date
        symbol_field_name: str
            symbol field name, default symbol
        Examples
        ---------
            $ python collector.py normalize_data --source_dir ~/.qlib/crypto_data/source/1d --normalize_dir ~/.qlib/crypto_data/source/1d_nor --interval 1d --date_field_name date
        """
super(Run, self).normalize_data(date_field_name, symbol_field_name)


if __name__ == "__main__":
  fire.Fire(Run)
