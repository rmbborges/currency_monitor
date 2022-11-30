from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

import logging
import requests
import asyncio
import datetime
import httpx
from typing import Any, Dict, List, Tuple
import sys

def convert_schema(
    types: Dict[str, Any],
    data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    converted_data = []
    for el in data:
        js = {}
        for key, value in el.items():
            if types[key] != "unix":
                value = types[key](value)
            else:
                value = datetime.datetime.utcfromtimestamp(int(value)).strftime("%Y-%m-%dT%H:%M:%S")
            js[key] = value
        converted_data.append(js)

    return converted_data

async def get_async(url):
    async with httpx.AsyncClient() as client:
        return await client.get(url)

        
class LoadCurrentCurrencyData(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        currencies=["USD", "EUR", "GBP", "JPY", "ARS", "CLP", "CHF", "AUD", "CNY", "ETH", "DOGE", "BTC"],
        postgres_conn_id="", 
        *args,
        **kwargs
    ):

        super(LoadCurrentCurrencyData, self).__init__(*args, **kwargs)
        self.currencies = currencies
        self.postgres_conn_id = postgres_conn_id

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
        converted_records = convert_schema(
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
                "timestamp": "unix",
                "create_date": str
            },
            data=records
        )

        pg_hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id,
            schema="finance"
        )

        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()

        for element in converted_records:
            insert_statement = pg_hook._generate_insert_sql(
                table="currency.data",
                values=tuple(element.values()),
                target_fields=["code", "codein", "name", "high", "low", "varBid", "pctChange", "bid", "ask", "timestamp", "created_date"],
                replace=False
            )
            logging.info(tuple(element.values()))
            cursor.execute(insert_statement, tuple(element.values()))

class LoadClosingReferenceData(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        currencies=["USD", "EUR", "GBP", "JPY", "ARS", "CLP", "CHF", "AUD", "CNY", "ETH", "DOGE", "BTC"],
        start_datetime=datetime.datetime.now() - datetime.timedelta(days=2),
        end_datetime=datetime.datetime.now() - datetime.timedelta(days=1),
        postgres_conn_id="", 
        *args,
        **kwargs
    ):

        super(LoadClosingReferenceData, self).__init__(*args, **kwargs)
        self.currencies = currencies
        self.postgres_conn_id = postgres_conn_id
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

    @staticmethod
    async def get_closing_reference_data(
        currencies,
        start_datetime,
        end_datetime
    ) -> List[Dict[str, Any]]:

        start_datetime_str = start_datetime.strftime("%Y%m%d")
        end_datetime_str = end_datetime.strftime("%Y%m%d")

        urls = [f"https://economia.awesomeapi.com.br/json/daily/{arg}/?start_date={start_datetime_str}&end_date={end_datetime_str}" for arg in currencies]

        resps = await asyncio.gather(*map(get_async, urls))
        data = [resp.json()[0] for resp in resps]
        return_content = []
        for content in data:
            value = round((float(content["bid"]) + float(content["ask"]))/2, 4)
            return_content.append({
                "code": content["code"],
                "value": value,
                "timestamp": end_datetime.timestamp()
            })
            
        return return_content

    
    def execute(self, context):
        records = asyncio.run(self.get_closing_reference_data(self.currencies, self.start_datetime, self.end_datetime))
        converted_records = convert_schema(
            types={
                "code": str,
                "value": float,
                "timestamp": "unix"
            },
            data=records
        )

        pg_hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id,
            schema="finance"
        )

        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()

        for element in converted_records:
            insert_statement = pg_hook._generate_insert_sql(
                table="currency.closing_reference",
                values=tuple(element.values()),
                target_fields=["code", "value", "timestamp"],
                replace=False
            )

            cursor.execute(insert_statement, tuple(element.values()))
