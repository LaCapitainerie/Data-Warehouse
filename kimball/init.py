import pandas as pd
import json
import psycopg2
import os
import requests
import numpy as np

def customers_to_stg(customers: pd.DataFrame, cursor: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    insert_query = f"""
    INSERT INTO stg_customers ({", ".join(customers.columns.to_list())})
    VALUES ({', '.join(['%s'] * len(customers.columns))})
    ON CONFLICT DO NOTHING;
    """
    cursor.executemany(insert_query, customers.replace(np.nan, None).values.tolist())
    conn.commit()
    print(f"stg_customers data inserted successfully!")

def order_items_to_stg(order_items: pd.DataFrame, cursor: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    insert_query = f"""
    INSERT INTO stg_order_items ({", ".join(order_items.columns.to_list())})
    VALUES ({', '.join(['%s'] * len(order_items.columns))})
    ON CONFLICT DO NOTHING;
    """
    cursor.executemany(insert_query, order_items.replace(np.nan, None).values.tolist())
    conn.commit()
    print(f"stg_order_items data inserted successfully!")

def products_to_stg(products: pd.DataFrame, cursor: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    insert_query = f"""
    INSERT INTO stg_products ({", ".join(products.columns.to_list())})
    VALUES ({', '.join(['%s'] * len(products.columns))})
    ON CONFLICT DO NOTHING;
    """
    cursor.executemany(insert_query, products.replace(np.nan, None).values.tolist())
    conn.commit()

def returns_to_stg(returns: pd.DataFrame, cursor: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    insert_query = f"""
    INSERT INTO stg_returns ({", ".join(returns.columns.to_list())})
    VALUES ({', '.join(['%s'] * len(returns.columns))})
    ON CONFLICT DO NOTHING;
    """
    cursor.executemany(insert_query, returns.replace(np.nan, None).values.tolist())
    conn.commit()
    print(f"stg_returns data inserted successfully!")


def main():

    conn = psycopg2.connect(
        host='localhost',
        database='kimball',
        user='postgres',
        password='postgres'
    )
    cursor = conn.cursor()



    # -- Customers --
    customers = pd.read_csv(os.path.dirname(__file__) + './../data/customers_1000.csv')
    customers_to_stg(customers, cursor, conn)
    # ----------------

    # -- Order Items --
    order_items = pd.read_csv(os.path.dirname(__file__) + './../data/order_list_15000.csv')
    order_items_to_stg(order_items, cursor, conn)
    # ----------------

    # -- Products --
    products = pd.read_json(os.path.dirname(__file__) + './../data/products_1000_fnac.json')
    products_to_stg(products, cursor, conn)
    # ----------------

    # -- Returns --
    returns = pd.read_csv(os.path.dirname(__file__) + './../data/returns_100.csv')
    returns_to_stg(returns, cursor, conn)
    # ----------------




    



    # -- Orders --
    orders = pd.read_csv(os.path.dirname(__file__) + './../data/orders_10000.csv')

    DEFAULT_CURRENCY = "EUR"

    start_date, end_date = orders['order_ts'].min(), orders['order_ts'].max()
    
    currency_json = requests\
       .get(f'https://api.frankfurter.dev/v1/{start_date}..{end_date}?base={DEFAULT_CURRENCY}')\
       .json()
    currency_rates = currency_json.get('rates', {})
    date_list = tuple(currency_rates.keys())

    def get_currency_rate(order_ts, currency):
        if currency == DEFAULT_CURRENCY:
            return 1
        if order_ts in currency_rates and currency in currency_rates[order_ts]:
            return currency_rates[order_ts][currency]
        else:
            # Find the previous and next available dates
            idx = np.searchsorted(date_list, order_ts)
            prev_idx = idx - 1 if idx > 0 else None
            next_idx = idx if idx < len(date_list) else None

            prev_date = date_list[prev_idx] if prev_idx is not None else None
            next_date = date_list[next_idx] if next_idx is not None and next_idx < len(date_list) else None

            prev_rate = currency_rates[prev_date][currency] if prev_date and currency in currency_rates[prev_date] else None
            next_rate = currency_rates[next_date][currency] if next_date and currency in currency_rates[next_date] else None

            available_rates = [r for r in [prev_rate, next_rate] if r is not None]

            if available_rates:
                return np.mean(available_rates)
            else:
                return np.nan


    orders['currency_rate'] = orders.apply(lambda row: get_currency_rate(row["order_ts"], row["currency"]), axis=1)

    insert_query = f"""
    INSERT INTO stg_orders ({", ".join(orders.columns.to_list())})
    VALUES ({', '.join(['%s'] * len(orders.columns))})
    ON CONFLICT DO NOTHING;
    """
    cursor.executemany(insert_query, orders.replace(np.nan, None).values.tolist())
    conn.commit()
    print(f"stg_orders data inserted successfully!")
    #----------------

if __name__ == "__main__":
    main()