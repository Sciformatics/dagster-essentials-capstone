from dagster import asset
from constants import RAW_DATA_PATH, DATE_TODAY
import requests
from dagster_duckdb import DuckDBResource
import pandas as pd
import datetime


@asset(
    description="The raw extract from the CoinCap Rates API."
)
def stg__extract_crypto_rates() -> None:

    response = requests.get('https://api.coincap.io/v2/rates').json()
    response_df = pd.json_normalize(response)
    response_df.to_csv(f'{RAW_DATA_PATH}/coincap_rates_api_{DATE_TODAY}.csv')

@asset(
    deps=['stg__extract_crypto_rates'],
    description="The transformation of the crypto data"
)
def nrm__transform_crypto_rates(database: DuckDBResource):

    query = f"""
        select * from '{RAW_DATA_PATH}/coincap_rates_api_{DATE_TODAY}';"""
    with database.get_connection() as conn:
        conn.execute(query)

    
