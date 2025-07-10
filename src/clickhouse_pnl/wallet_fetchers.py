from typing import List, Dict
from src.sdk.databases.clickhouse.click_connect import get_db


def _rows_to_dict(rows) -> List[Dict]:
    """ClickHouse чаще отдаёт кортежи; нормализуем в [{signing_wallet: …}, …]."""
    if rows and not isinstance(rows[0], dict):
        rows = [{"signing_wallet": r[0]} for r in rows]
    return rows


def fetch_pumpswap_pnl(token_address: str) -> List[Dict]:
    """Старая функция — перенесена сюда без изменений."""
    db = get_db()
    query = """
        SELECT
            signing_wallet,
            sumIf(quote_token_amount, direction = 'S')
              - sumIf(quote_token_amount, direction = 'B')
              - sum(lp_fee + protocol_fee + fee)   AS pnl_quote,
            countIf(direction = 'B')               AS buys
        FROM pumpswap_all_swaps
        WHERE base_token = {token:String}
        GROUP BY signing_wallet
        HAVING buys < 5 AND pnl_quote > 200000000
        ORDER BY pnl_quote DESC
    """
    rows = db.fetchall(query, params={"token": token_address})
    # нормализуем только если ClickHouse вернул кортежи
    if rows and not isinstance(rows[0], dict):
        keys = ("signing_wallet", "pnl_quote", "buys")
        rows = [dict(zip(keys, r)) for r in rows]
    return rows


def fetch_raydium_wallets(token_address: str) -> List[Dict]:

    db = get_db()

    query = """
        WITH swaps AS (
            SELECT
                fee_payer,
                /* +quote, если 'S' (продажа базы); –quote, если не 'S' */
                CASE
                    WHEN direction = 'S'
                        THEN toInt128(quote_coin_amount)
                    ELSE
                        -toInt128(quote_coin_amount)
                END AS delta_quote
            FROM raydium_cpmm_swaps
            WHERE base_coin = {token:String}
        )
        SELECT
            fee_payer,
            sum(delta_quote) AS pnl_quote_coin,
            count()          AS trades_cnt
        FROM swaps
        GROUP BY fee_payer
        HAVING pnl_quote_coin > 0
        ORDER BY pnl_quote_coin DESC
    """

    rows = db.fetchall(query, params={"token": token_address})
    return _rows_to_dict(rows)


def fetch_meteora_wallets(token_address: str) -> List[Dict]:
    """Meteora: берём обмены в обе стороны."""
    db = get_db()
    query = """
        SELECT DISTINCT signing_wallet
        FROM meteora_swaps
        WHERE base_coin  = {token:String}
           OR quote_coin = {token:String}
    """
    rows = db.fetchall(query, params={"token": token_address})
    return _rows_to_dict(rows)
