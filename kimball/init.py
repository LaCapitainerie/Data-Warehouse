from pandas import DataFrame, read_csv, read_json
from psycopg2 import connect, extensions
from os import path
from requests import get
from numpy import searchsorted, mean, nan

def customers_to_stg(customers: DataFrame, cursor: extensions.cursor, conn: extensions.connection):
    insert_query = f"""
    INSERT INTO stg_customers ({", ".join(customers.columns.to_list())})
    VALUES ({', '.join(['%s'] * len(customers.columns))})
    ON CONFLICT DO NOTHING;
    """
    cursor.executemany(insert_query, customers.replace(nan, None).values.tolist())
    conn.commit()
    print(f"stg_customers data inserted successfully!")

def order_items_to_stg(order_items: DataFrame, cursor: extensions.cursor, conn: extensions.connection):
    insert_query = f"""
    INSERT INTO stg_order_items ({", ".join(order_items.columns.to_list())})
    VALUES ({', '.join(['%s'] * len(order_items.columns))})
    ON CONFLICT DO NOTHING;
    """
    cursor.executemany(insert_query, order_items.replace(nan, None).values.tolist())
    conn.commit()
    print(f"stg_order_items data inserted successfully!")

def products_to_stg(products: DataFrame, cursor: extensions.cursor, conn: extensions.connection):
    insert_query = f"""
    INSERT INTO stg_products ({", ".join(products.columns.to_list())})
    VALUES ({', '.join(['%s'] * len(products.columns))})
    ON CONFLICT DO NOTHING;
    """
    cursor.executemany(insert_query, products.replace(nan, None).values.tolist())
    conn.commit()

    print(f"stg_products data inserted successfully!")

def returns_to_stg(returns: DataFrame, cursor: extensions.cursor, conn: extensions.connection):
    insert_query = f"""
    INSERT INTO stg_returns ({", ".join(returns.columns.to_list())})
    VALUES ({', '.join(['%s'] * len(returns.columns))})
    ON CONFLICT DO NOTHING;
    """
    cursor.executemany(insert_query, returns.replace(nan, None).values.tolist())
    conn.commit()
    print(f"stg_returns data inserted successfully!")


def main():

    conn = connect(
        host='localhost',
        database='kimball',
        user='postgres',
        password='postgres'
    )
    cursor = conn.cursor()

    print(f"Connected to the database successfully!")
    cursor.execute("TRUNCATE TABLE stg_customers, stg_products, stg_returns, stg_orders, stg_order_items;")
    conn.commit()
    print(f"Tables truncated successfully!")


    # -- Customers --
    customers = read_csv(path.dirname(__file__) + './../data/customers_1000.csv')
    customers_to_stg(customers, cursor, conn)
    # ----------------

    # -- Products --
    products = read_json(path.dirname(__file__) + './../data/products_1000_fnac.json')
    products_to_stg(products, cursor, conn)
    # ----------------

    # -- Returns --
    returns = read_csv(path.dirname(__file__) + './../data/returns_100.csv')
    returns_to_stg(returns, cursor, conn)
    # ----------------




    



    # -- Orders --
    orders = read_csv(path.dirname(__file__) + './../data/orders_10000.csv')

    DEFAULT_CURRENCY = "EUR"

    start_date, end_date = orders['order_ts'].min(), orders['order_ts'].max()
    
    currency_json = get(f'https://api.frankfurter.dev/v1/{start_date}..{end_date}?base={DEFAULT_CURRENCY}').json()
    currency_rates = currency_json.get('rates', {})
    date_list = tuple(currency_rates.keys())

    def get_currency_rate(order_ts, currency):
        if currency == DEFAULT_CURRENCY:
            return 1
        if order_ts in currency_rates and currency in currency_rates[order_ts]:
            return currency_rates[order_ts][currency]
        else:
            # Find the previous and next available dates
            idx = searchsorted(date_list, order_ts)
            prev_idx = idx - 1 if idx > 0 else None
            next_idx = idx if idx < len(date_list) else None

            prev_date = date_list[prev_idx] if prev_idx is not None else None
            next_date = date_list[next_idx] if next_idx is not None and next_idx < len(date_list) else None

            prev_rate = currency_rates[prev_date][currency] if prev_date and currency in currency_rates[prev_date] else None
            next_rate = currency_rates[next_date][currency] if next_date and currency in currency_rates[next_date] else None

            available_rates = [r for r in [prev_rate, next_rate] if r is not None]

            if available_rates:
                return mean(available_rates)
            else:
                return nan

    orders['currency_rate'] = orders.apply(lambda row: get_currency_rate(row["order_ts"], row["currency"]), axis=1)

    insert_query = f"""
    INSERT INTO stg_orders ({", ".join(orders.columns.to_list())})
    VALUES ({', '.join(['%s'] * len(orders.columns))})
    ON CONFLICT DO NOTHING;
    """
    cursor.executemany(insert_query, orders.replace(nan, None).values.tolist())
    conn.commit()
    print(f"stg_orders data inserted successfully!")
    #----------------




    # -- Order Items --
    order_items = read_csv(path.dirname(__file__) + './../data/order_list_15000.csv')
    order_items_to_stg(order_items, cursor, conn)
    # ----------------

if __name__ == "__main__":
    main()