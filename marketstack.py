'''
########################################################################################################################
Make sure to update the API_KEY variable with your own API key. You can get an API key by logging into your account at https://www.marketstack.com/ or making an account at https://www.marketstack.com/signup.
########################################################################################################################
'''

import os
from dotenv import load_dotenv

load_dotenv()

marketstack_api_key = os.getenv("MARKETSTACK_API_KEY", "YOUR_API_KEY")

if not marketstack_api_key:
    raise ValueError("API Key not set. Please set your API Key in your .env file with the MARKETSTACK_API_KEY variable")

def set_api_key(api_key):
    global marketstack_api_key
    marketstack_api_key = api_key

    os.environ['MARKETSTACK_API_KEY'] = api_key

'''
########################################################################################################################
Don't touch anything beneath this line. All you need to do is add your API Key to your environment variables.
########################################################################################################################
'''

#TODO - Add chainable function calls for the features 

import requests
import pandas as pd
from typing import Optional, List, Union
from datetime import datetime

ARG_EXCEPTIONS = ['self', '__class__']

def prep_args(args:dict, only_keys:List[str] =None):
    if only_keys:
        return {key: value for key, value in args.items() if key in only_keys and key not in ARG_EXCEPTIONS}
    else:
        return {key: value for key, value in args.items() if key not in ARG_EXCEPTIONS}

class MarketStack:
    def __init__(self, endpoint: str, symbols: Optional[str] = None, exchange: Optional[str] = None, sort: Optional[str] = None, date_from: Optional[str] = None, date_to: Optional[str] = None, limit: Optional[str] = None, offset: Optional[str] = None, search: Optional[str] = None):
        self.base_url = "http://api.marketstack.com/v1/"
        self.url = f"http://api.marketstack.com/v1/{self.validate_endpoint(endpoint)}"
        self.endpoint = endpoint
        #define parameters for API call. Making sure the API Key is always included.
        self.params = {
            'access_key': marketstack_api_key
        }
        self.feature_support = {
            'eod': ['latest', 'historical'],
            'intraday': ['latest', 'historical'],
            'splits': [],
            'dividends': [],
            'tickers': ['eod'],
            'exchanges': [],
            'currencies': [],
            'timezones': []
        }

        if symbols:
            self.params['symbols'] = symbols
        if exchange:
            self.params['exchange'] = exchange
        if sort:
            self.params['sort'] = self.validate_sort(sort)
        if date_from:
            self.params['date_from'] = self.validate_date(date_from)
        if date_to:
            self.params['date_to'] = self.validate_date(date_to)
        if limit:
            self.params['limit'] = self.validate_limit(limit)
        if offset:
            self.params['offset'] = self.validate_offset(offset)
        if search:
            self.params['search'] = self.validate_search(search)

    #Feature Functions
    def latest(self):
        self.__validate_feature_support('latest')
        self.url += '/latest'
        return self
    
    def historical(self, date_from: Union[datetime, str], date_to: Optional[Union[datetime, str]] = None):
        self.__validate_feature_support('historical')
        #if only one date is provided, it will be assumed to be the date_from date and passed as a historical feature.
        if date_to is None: 
            self.url += f'/{self.validate_date(date_from)}'
            return self
        
        #Check if date_from and date_to are already defined in parameters. If not, add them.
        if 'date_from' in self.params or 'date_to' in self.params:
            raise ValueError("Date_from and Date_to cannot be specified more than once.")
        
        self.params['date_from'] = self.validate_date(date_from)
        self.params['date_to'] = self.validate_date(date_to)
        return self
    

    def __validate_feature_support(self, feature):
        if feature in self.feature_support[self.endpoint]:
            return feature
        else:
            raise ValueError(f'Endpoint Feature: "{feature}" is not supported for the "{self.endpoint}" endpoint.')

    #Helpers
    def reset_url(self):
        self.url = self.base_url + self.validate_endpoint(self.endpoint)

    # Validators
    def validate_endpoint(self, endpoint):
        valid_endpoints = ["eod", "intraday", "splits", "dividends", "tickers", "exchanges", "currencies", "timezones"]
        if endpoint in valid_endpoints:
            return endpoint
        else:
            raise ValueError("Endpoint: ", endpoint ," not supported.")
        
    def validate_date(self, date: Union[datetime, str]) -> str:
        if isinstance(date, datetime):
            self.__maybe_strip_time(date)

        # Valid formats for marketstack dates are 'YYYY-MM-DD' and 'YYYY-MM-DDTHH:MM:SS'
        formats = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date, fmt)

                if fmt == "%Y-%m-%dT%H:%M:%S":
                    return self.__maybe_strip_time(parsed_date)
                
                return date
            except ValueError:
                continue
        raise ValueError("Date must be in YYYY-MM-DD or ISO-8601 (YYYY-MM-DDTHH:MM:SS) format.")
    
    def __maybe_strip_time(self, date: datetime):
        # If time is midnight, return date only
        if date.time() == datetime.time(0, 0):
            return date.strftime("%Y-%m-%d")
        return date.isoformat()
        
    def validate_sort(self, sort):
        if sort not in ["asc", "desc"]:
            raise ValueError("Sort parameter must be 'asc' or 'desc'.")
        return sort

    def validate_limit(self, limit):
        if not 1 <= limit <= 1000:
            raise ValueError("Limit parameter must be between 1 and 1000.")
        return limit

    def validate_offset(self, offset):
        if not 0 <= offset <= 1000:
            raise ValueError("Offset parameter must be between 0 and 1000.")
        return offset
        
    def validate_search(self, search):
        searchable_endpoints = ["tickers", "exchange"]
        if self.endpoint not in searchable_endpoints:
            raise ValueError(f'Search is not supported for the "{self.endpoint}" endpoint. Search is only supported for the following endpoints: {searchable_endpoints}')
        self.params['search'] = search

    
    #Request Functions
    def request(self):
        assert marketstack_api_key != "YOUR_API_KEY", "Please update your API Key."
        response = requests.get(self.url, self.params)
        return response
    
    def get_http_response_code(self):
        api_response = self.request()
        return api_response.status_code

    def get_api_response(self):
        api_response = self.request()
        return api_response.json()

    def get_data(self):
        try:
            raw_response = self.request()
            raw_response.raise_for_status()  # Raises an HTTPError if the status is 4xx, 5xx
        except requests.exceptions.HTTPError as http_err:
            api_response = raw_response.json()
            error_message = f"HTTP error occurred: {http_err} - {api_response.get('error', {}).get('message', 'No error message')}"
            raise ValueError(error_message) from http_err
        except requests.exceptions.RequestException as err:
            # Handle random errors
            raise SystemExit(f"Request error occurred: {err}") from err

        api_response = raw_response.json()

        #Check for pagination key in response. If it exists, the 'data' key will be a list of dictionaries. If not the response will just be the one dictionary.
        if api_response.get('pagination'):
            #TODO - Add support for pagination traversal
            return api_response['data']
        
        return api_response
    
    def get_data_df(self):
        data = self.get_data()
        df = pd.DataFrame(data)
        return df


class EndOfDay(MarketStack):
    def __init__(self, symbols: str, exchange: Optional[str] = None, sort: Optional[str] = None, date_from: Optional[str] = None, date_to: Optional[str] = None, limit: Optional[str] = None, offset: Optional[str] = None):
        args = prep_args(locals())
        super().__init__('eod', **args)

class Intraday(MarketStack):
    def __init__(self, symbols: str, exchange: Optional[str] = None, sort: Optional[str] = None, date_from: Optional[str] = None, date_to: Optional[str] = None, limit: Optional[str] = None, offset: Optional[str] = None):
        args = prep_args(locals())
        super().__init__('intraday', **args)

class Splits(MarketStack):
    def __init__(self, symbols: str, sort: Optional[str] = None, date_from: Optional[str] = None, date_to: Optional[str] = None, limit: Optional[str] = None, offset: Optional[str] = None):
        args = prep_args(locals())
        super().__init__('splits', **args)

class Dividends(MarketStack):
    def __init__(self, symbols: str, sort: Optional[str] = None, date_from: Optional[str] = None, date_to: Optional[str] = None, limit: Optional[str] = None, offset: Optional[str] = None):
        args = prep_args(locals())
        super().__init__('dividends', **args)

class Currencies(MarketStack):
    def __init__(self, limit: Optional[int] = None, offset: Optional[int] = None):
        args = prep_args(locals())
        super().__init__('currencies', **args)

class TimeZones(MarketStack):
    def __init__(self, limit: Optional[int] = None, offset: Optional[int] = None):
        args = prep_args(locals())
        super().__init__('timezones', **args)

class Tickers(MarketStack):
    def __init__(self, get_symbol: Optional[str] = None, exchange: Optional[str] = None, search: Optional[str] = None, limit: Optional[int] = None, offset: Optional[int] = None, get_splits: bool = False, get_dividends: bool = False, get_eod: bool = False, get_intraday: bool = False, date: Optional[str] = None, get_latest: bool = False):
        args = prep_args(locals(), only_keys=['exchange', 'search', 'limit', 'offset'])
        super().__init__('tickers', **args)

        
        self.get_symbol = get_symbol
        self.get_splits = get_splits
        self.get_dividends = get_dividends
        self.get_eod = get_eod
        self.get_intraday = get_intraday
        self.date = date
        self.get_latest = get_latest

        self.validate_inputs()
        self.build_url()

    def symbol(self, symbol: str):
        self.get_symbol = symbol

        self.validate_inputs()
        self.build_url()

        return self

    def eod(self):
        #TODO - add EOD endpoint parameter support
        self.get_eod = True

        self.validate_inputs()
        self.build_url()

        return self
    
    def intraday(self):
        #TODO - add intraday endpoint parameter support
        self.get_intraday = True

        self.validate_inputs()
        self.build_url()

        return self
    
    def splits(self):
        self.get_splits = True

        self.validate_inputs()
        self.build_url()

        return self
    
    def dividends(self):
        self.get_dividends = True

        self.validate_inputs()
        self.build_url()

        return self
    
    def latest(self):
        self.latest = True

        self.validate_inputs()
        self.build_url()

        return self
    
    def historical(self, date: Union[datetime, str]):
        self.date = date

        self.validate_inputs()
        self.build_url()

        return self
    
    def validate_inputs(self):
        if any([self.get_splits, self.get_dividends, self.get_eod, self.get_intraday, self.date, self.get_latest]) and not self.get_symbol:
            raise ValueError("get_symbol must be specified if get_splits, get_dividends, get_eod, get_intraday, date, or get_latest are specified.")
        if self.date and self.get_latest:
            raise ValueError("Date and get_latest cannot be specified at the same time.")
        if self.get_intraday and self.date:
            raise ValueError("Date cannot be specified with get_intraday. Please set a to_date and from_date instead.")
        if self.get_latest and not (self.get_eod or self.get_intraday):
            raise ValueError("get_eod or get_intraday must be specified if get_latest is specified.")
        if sum([self.get_splits, self.get_dividends, self.get_eod, self.get_intraday]) > 1:
            raise ValueError("get_splits, get_dividends, get_eod, and get_intraday cannot be specified together.")
        if (self.get_splits or self.get_dividends) and (self.date or self.get_latest):
            raise ValueError("get_splits and get_dividends cannot be specified with Date or get_latest.")

    def build_url(self):
        self.reset_url()
        self.url += f'/{self.get_symbol}' if self.get_symbol else ''
        self.url += '/splits' if self.get_splits else ''
        self.url += '/dividends' if self.get_dividends else ''
        self.url += '/eod' if self.get_eod else ''
        self.url += '/intraday' if self.get_intraday else ''
        self.url += f'/{self.validate_date(self.date)}' if self.date else ''
        self.url += '/latest' if self.get_latest else ''


class Exchanges(MarketStack):
    def __init__(self, search: Optional[str] = None, limit: Optional[int] = None, offset: Optional[int] = None, mic: Optional[str] = None, get_tickers: Optional[bool] = False, get_eod: Optional[bool] = False, get_intraday: Optional[bool] = False, date: Optional[str] = None, get_latest: Optional[bool] = False):
        args = prep_args(locals(), only_keys=['search', 'limit', 'offset'])
        super().__init__('exchanges', **args)

        self.mic = mic
        self.get_tickers = get_tickers
        self.get_eod = get_eod
        self.get_intraday = get_intraday
        self.date = date
        self.get_latest = get_latest

        self.validate_inputs()
        self.build_url()

    def exchange(self, mic: str):
        self.mic = mic

        self.validate_inputs()
        self.build_url()

        return self
    
    def tickers(self):
        self.get_tickers = True

        self.validate_inputs()
        self.build_url()

        return self
    
    def eod(self):
        #TODO - add EOD endpoint parameter support
        self.get_eod = True

        self.validate_inputs()
        self.build_url()

        return self
    
    def intraday(self):
        #TODO - add intraday endpoint parameter support
        self.get_intraday = True

        self.validate_inputs()
        self.build_url()

        return self
    
    def latest(self):
        self.latest = True

        self.validate_inputs()
        self.build_url()

        return self
    
    def historical(self, date: Union[datetime, str]):
        self.date = date

        self.validate_inputs()
        self.build_url()

        return self
    

    def validate_inputs(self):
        if any([self.get_tickers, self.get_eod, self.get_intraday, self.date, self.get_latest]) and not self.mic:
            raise ValueError("mic must be specified if get_tickers, get_eod, get_intraday, date, or get_latest are specified.")
        if self.date and self.get_latest:
            raise ValueError("Date and get_latest cannot be specified at the same time.")
        if (self.date or self.get_latest) and not (self.get_eod or self.get_intraday):
            raise ValueError("get_eod or get_intraday must be specified if Date or get_latest is specified.")
        if sum([self.get_tickers, self.get_eod, self.get_intraday]) > 1:
            raise ValueError("get_tickers, get_eod, and get_intraday cannot be specified together.")
        if self.get_tickers and (self.date or self.get_latest):
            raise ValueError("get_tickers cannot be specified with Date and get_latest.")

    def build_url(self):
        self.reset_url()
        if self.mic:
            self.url += f'/{self.mic}'
        if self.get_tickers:
            self.url += '/tickers'
        if self.get_eod:
            self.url += '/eod'
        if self.get_intraday:
            self.url += '/intraday'
        if self.date:
            self.url += f'/{self.validate_date(self.date)}'
        if self.get_latest:
            self.url += '/latest'


