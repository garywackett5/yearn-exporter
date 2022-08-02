
from datetime import datetime, timedelta
from typing import Optional

from pandas import (Categorical, DataFrame, MultiIndex, concat, pivot_table,
                    set_option, to_datetime)
from pony.orm import db_session, select
from tqdm import tqdm
from yearn.entities import TreasuryTx
from yearn.treasury.accountant.constants import PENDING_LABEL, treasury
from yearn.treasury.accountant.cost_of_revenue import COR_LABEL
from yearn.treasury.accountant.expenses import OPEX_LABEL
from yearn.treasury.accountant.other_expenses import OTHER_EXPENSE_LABEL
from yearn.treasury.accountant.other_income import OTHER_INCOME_LABEL
from yearn.treasury.accountant.revenue import REVENUE_LABEL

set_option('display.float_format', lambda x: '%.2f' % x)


def main():
    df = prep_df(fetch_data_from_db())
    print(df)

def mtd():
    bom = beginning_of_month(datetime.now())
    df = prep_df(fetch_data_from_db(from_timestamp=bom.timestamp()))
    print(df)

def qtd():
    boq = beginning_of_quarter(datetime.now())
    df = fetch_data_from_db(from_timestamp=boq.timestamp())
    df = prep_df(df)
    print(df)

def last_month():
    one_month_ago = this_time_last_month(datetime.now())
    bom = beginning_of_month(one_month_ago)
    eom = end_of_month(one_month_ago)
    df = fetch_data_from_db(from_timestamp=bom.timestamp(), to_timestamp=eom.timestamp())
    df = prep_df(df)
    print(df)

def last_quarter():
    start_of_current_quarter = beginning_of_quarter(datetime.now())
    dt = this_time_last_month(datetime.now())
    for _ in range(3):
        if dt < start_of_current_quarter:
            boq = beginning_of_quarter(dt)
            break
        else:
            dt = this_time_last_month(dt)
    eoq = end_of_quarter(dt)
    print(eoq)
    df = fetch_data_from_db(from_timestamp=boq.timestamp(), to_timestamp=eoq.timestamp())
    df = prep_df(df)
    print(df)
    df.to_csv(f'./reports/pnl_{boq.date()}_{eoq.date()}.csv')

@db_session
def fetch_data_from_db(from_timestamp: Optional[int] = None, to_timestamp: Optional[int] = None):
    if from_timestamp is None and to_timestamp is None:
        txs = select((tx.timestamp, tx.value_usd, tx.chain.chain_name, tx.txgroup, tx.from_address.address) for tx in TreasuryTx)
    elif from_timestamp is None and to_timestamp is not None:
        txs = select((tx.timestamp, tx.value_usd, tx.chain.chain_name, tx.txgroup, tx.from_address.address) for tx in TreasuryTx if tx.timestamp <= to_timestamp)
    elif from_timestamp is not None and to_timestamp is None:
        txs = select((tx.timestamp, tx.value_usd, tx.chain.chain_name, tx.txgroup, tx.from_address.address) for tx in TreasuryTx if tx.timestamp >= from_timestamp)
    elif from_timestamp is not None and to_timestamp is not None:
        txs = select((tx.timestamp, tx.value_usd, tx.chain.chain_name, tx.txgroup, tx.from_address.address) for tx in TreasuryTx if tx.timestamp >= from_timestamp and tx.timestamp < to_timestamp)
    return DataFrame([
        {
            'timestamp': tx[0],
            'value_usd': tx[1],
            'chain': tx[2],
            'from': tx[4],
            'txgroup': tx[3].name if tx[3].name != PENDING_LABEL else f"{PENDING_LABEL} - out" if tx[4] in treasury.addresses else f"{PENDING_LABEL} - in",
            'top': tx[3].top_txgroup.name,
        } for tx in tqdm(txs) if tx[3].top_txgroup.name != "Ignore"
    ])
    
def prep_df(df: DataFrame):
    # Timestamp must be datetime.
    df.timestamp = to_datetime(df.timestamp, unit='s')
    df.top = Categorical(df.top, [REVENUE_LABEL, COR_LABEL, OPEX_LABEL, OTHER_INCOME_LABEL, OTHER_EXPENSE_LABEL, PENDING_LABEL])

    df = pivot_table(
        df,
        ['value_usd'],
        'timestamp',
        ['chain','txgroup','top'],
        'sum',
    ).resample('1M').sum().stack().stack().stack()

    df = pivot_table(
        df,
        ['value_usd'],
        ['top','chain','txgroup'],
        ['timestamp',],
    ).fillna(0)

    df = df.loc[(df!=0).any(axis=1)]

    subtotals = df.groupby(level=0, sort=False).sum()    
    subtotals.index = MultiIndex.from_tuples(
        zip(
            df.index.levels[0].categories,
            [f"Total {category}" for category in subtotals.index],
            ["" for _ in subtotals.index],
        )
    )

    return concat([df, subtotals]).sort_index()

def beginning_of_month(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, 1)

def beginning_of_quarter(dt: datetime) -> datetime:
    return datetime(dt.year, (dt.month - 1) // 3 * 3 + 1 if dt.month != 12 else 10, 1)

def beginning_of_next_month(dt: datetime) -> datetime:
    return datetime(dt.year if dt.month != 12 else dt.year + 1, dt.month + 1 if dt.month != 12 else 1, 1)

def beginning_of_next_quarter(dt: datetime) -> datetime:
    return datetime(dt.year, (dt.month - 1) // 3 * 3 + 4 if dt.month != 12 else 10, 1)

def end_of_month(dt: datetime) -> datetime:
    return beginning_of_next_month(dt) - timedelta(microseconds=1)
    
def end_of_quarter(dt: datetime) -> datetime:
    return beginning_of_next_quarter(dt) - timedelta(microseconds=1)

def this_time_last_month(dt: datetime) -> datetime:
    return datetime(
        dt.year if dt.month > 1 else dt.year - 1,
        dt.month - 1 if dt.month > 1 else 12,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
        dt.microsecond
    )
