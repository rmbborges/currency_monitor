from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import logging
import requests
import json
import logging
import requests
from typing import Any, Dict, List
import sys

def convert_schema(
        types: Dict[str, Any],
        data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        converted_data = []
        for el in data:
            js = {}
            for key, value in el.items():
                value = types[key](value)
                js[key] = value
            converted_data.append(js)

        return converted_data
        
class LoadCurrentCurrencyData(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        currencies=["USD", "EUR", "GBP", "JPY", "ARS", "CLP", "CHF", "AUD", "CNY", "ETH", "DOGE", "BTC"],
        postgres_connection_id="", 
        *args,
        **kwargs
    ):

        super(LoadCurrentCurrencyData, self).__init__(*args, **kwargs)
        self.currencies = currencies
        self.postgres_connection_id = postgres_connection_id

    @staticmethod
    def get_current_currency_data(
        currencies
    ) -> List[Dict[str, Any]]:
        arg = ",".join([f"{c}-BRL" for c in currencies])
        url = f'https://economia.awesomeapi.com.br/last/{arg}'
        try:
            r = requests.get(url)
        except requests.ConnectionError as ce:
            logging.error(f"There was an error with the request, {ce}")
            sys.exit(1)
        response = r.json()
        records = [response[k] for k in response.keys()]

        return records

    
    def execute(self, context):
        records = self.get_current_currency_data(self.currencies)
        converted_records = self.convert_schema(
            types={
                "code": str,
                "codein": str,
                "name": str,
                "high": float,
                "low": float,
                "varBid": float,
                "pctChange": float,
                "bid": float,
                "ask": float,
                "timestamp": int,
                "create_date": str
            },
            data=records
        )


