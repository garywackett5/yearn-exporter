
from pony.orm import select, db_session
from pandas import DataFrame #, pivot_table, to_datetime
from dask.dataframe import from_pandas, pivot_table, to_datetime, to_numeric
from yearn.treasury.accountant.accountant import all_txs
from tqdm import tqdm
from datetime import datetime
from yearn.treasury.accountant.ignore import IGNORE_LABEL
from yearn.entities import TreasuryTx


@db_session
def fetch_data_from_db():
    df = from_pandas(
        DataFrame([
            {
                "chain": tx.chain.chain_name,
                "timestamp": tx.timestamp,
                "block": tx.block,
                "from_address": tx.from_address.address,
                "from_nickname": tx.from_address.nickname,
                "to_address": tx.to_address.address if tx.to_address else None,
                "to_nickname": tx.to_address.nickname if tx.to_address else None,
                "symbol": tx.token.symbol,
                "token_address": tx.token.address.address,
                "amount": tx.amount,
                "value_usd": tx.value_usd,
                "txgroup": tx.txgroup.full_string,
                "top_level_txgroup": tx.txgroup.top_txgroup.name,
            }
            for tx in tqdm(all_txs()) if tx.txgroup.top_txgroup.name not in [IGNORE_LABEL] and datetime(2022,4,1) <= datetime.fromtimestamp(tx.timestamp) < datetime(2022,7,1)
        ]),
        chunksize=10_000
    )
    df.timestamp = to_datetime(df.timestamp, unit='s')
    df.amount = to_numeric(df.amount)
    df.value_usd = to_numeric(df.value_usd)
    return df
    

def main():
    df = fetch_data_from_db().compute()
    print(df)
    df = df.groupby(['chain','symbol','token_address','txgroup','top_level_txgroup']).resample('1D', on='timestamp').sum().reset_index()
    df = df.drop(columns=['block'])
    df['month'] = df.timestamp.dt.month
    df['year'] = df.timestamp.dt.year
    print(df.columns)
    print(df)
    '''
    df = pivot_table(
        df,
        ['amount','value_usd'],
        'timestamp',
        ['chain','symbol','token_address','txgroup','top_level_txgroup'],
        'sum',
    ).resample('1D').sum().stack(['chain','symbol','token_address','txgroup','top_level_txgroup']).compute()
    print(df)
    '''
    df.to_csv('./reports/txs_dump.csv')

    """
    # Group and sum data.
    grouped = pivot_table(
        df,
        ['amount','value_usd'],
        'timestamp',
        ['token','chain'],
        'sum',
    ).resample('1M').sum().stack(['chain','token'],dropna=True)
    """
