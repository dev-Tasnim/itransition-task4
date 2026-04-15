import pandas as pd
import yaml
import re
import networkx as nx
import matplotlib.pyplot as plt
from itertools import combinations
import datetime

def process_folder(folder_name):
    if not folder_name.endswith('/') and not folder_name.endswith('\\'):
        folder_name += '/'
    
    print(f"--- Processing {folder_name} ---")
    
    users = pd.read_csv(f'{folder_name}users.csv', skipinitialspace=True)
    users = users.replace(['NULL', ' ', ''], pd.NA)

    with open(f'{folder_name}books.yaml', 'r') as f:
        books_data = yaml.safe_load(f)
    books = pd.DataFrame(books_data)
    books.columns = [col.lstrip(':') for col in books.columns]

    orders = pd.read_parquet(f'{folder_name}orders.parquet')

    
    orders['timestamp'] = pd.to_datetime(orders['timestamp'], errors='coerce', utc=True)
    
    orders = orders.dropna(subset=['timestamp'])
    
    orders['timestamp'] = orders['timestamp'].dt.tz_localize(None)
    orders['date'] = orders['timestamp'].dt.date
    
    today_val = datetime.date.today()
    orders = orders[orders['date'] < today_val]
    
    orders['shipping'] = orders['shipping'].replace(['NULL', ' ', ''], pd.NA)
    orders = orders.dropna(subset=['book_id', 'user_id'])
    users = users.drop_duplicates()
    books = books.drop_duplicates(subset=['id']) 
    orders = orders.drop_duplicates()
    orders['quantity'] = pd.to_numeric(orders['quantity'], errors='coerce').fillna(0)

    def convert_to_usd(price_str):
        if pd.isna(price_str): return 0.0
        price_str = str(price_str).upper().strip()
        is_euro = '€' in price_str or 'EUR' in price_str
        clean_str = price_str.replace('¢', '.')
        clean_str = re.sub(r'[^0-9.]', '', clean_str)
        if clean_str.endswith('.'): clean_str = clean_str[:-1]
        try:
            value = float(clean_str)
            return round(value * 1.2, 2) if is_euro else round(value, 2)
        except: return 0.0

    orders['unit_price_usd'] = orders['unit_price'].apply(convert_to_usd)
    orders['paid_price'] = orders['quantity'] * orders['unit_price_usd']

    
    daily_revenue = orders.groupby('date')['paid_price'].sum()
    top_5_days = daily_revenue.sort_values(ascending=False).head(5)

    def get_real_user_count(df):
        G = nx.Graph()
        G.add_nodes_from(df['id'].tolist())
        cols = ['name', 'address', 'phone', 'email']
        for combo in combinations(cols, 3):
            grouped = df.dropna(subset=list(combo)).groupby(list(combo))['id'].apply(list)
            for id_list in grouped:
                if len(id_list) > 1:
                    for i in range(len(id_list) - 1):
                        G.add_edge(id_list[i], id_list[i+1])
        return list(nx.connected_components(G))

    user_clusters = get_real_user_count(users)
    num_real_users = len(user_clusters)

    id_to_master = {uid: min(cluster) for cluster in user_clusters for uid in cluster}
    orders['master_user_id'] = orders['user_id'].map(id_to_master)

    
    unique_author_sets_count = books['author'].nunique()
    sales_with_authors = orders.merge(books, left_on='book_id', right_on='id')
    most_popular_author = sales_with_authors.groupby('author')['quantity'].sum().idxmax()
    
    customer_spending = orders.groupby('master_user_id')['paid_price'].sum()
    top_customer_master_id = customer_spending.idxmax()
    top_customer_aliases = next(list(c) for c in user_clusters if top_customer_master_id in c)

    
    daily_revenue.index = pd.to_datetime(daily_revenue.index)
    
    
    data_key = folder_name.strip('./').strip('/').split('/')[-1]
    
    if not daily_revenue.empty:
        all_dates = pd.date_range(start=daily_revenue.index.min(), end=daily_revenue.index.max())
        plot_data = daily_revenue.reindex(all_dates, fill_value=0.0)

        plt.figure(figsize=(12, 6))
        plt.plot(plot_data.index, plot_data.values, marker='', linestyle='-', color='#1f77b4', linewidth=2)
        plt.title(f'Daily Revenue - {data_key}', fontsize=16, fontweight='bold')
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Revenue ($)', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'daily_revenue_{data_key}.png')
        plt.close()

    return {
        "top_5": top_5_days,
        "users": num_real_users,
        "authors": unique_author_sets_count,
        "popular_author": most_popular_author,
        "best_buyer": top_customer_aliases
    }


results = {}
for folder in ["DATA1", "DATA2", "DATA3"]:
    try:
        results[folder] = process_folder(f"./data/{folder}/")
    except Exception as e:
        print(f"Error processing {folder}: {e}")