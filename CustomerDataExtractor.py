import pickle
import pandas as pd

class CustomerDataExtractor:
    def __init__(self, pickle_path, vip_path, output_csv):
        self.pickle_path = pickle_path
        self.vip_path = vip_path
        self.output_csv = output_csv

    def load_pickle(self):
        with open(self.pickle_path, 'rb') as f:
            return pickle.load(f)

    def load_vip_ids(self):
        with open(self.vip_path, 'r') as f:
            return set(int(line.strip()) for line in f if line.strip().isdigit())

    def flatten_data(self, data, vip_ids):
        rows = []
        for customer in data:
            cid = customer.get('id')
            name = customer.get('name')
            reg_date = customer.get('registration_date')
            is_vip = cid in vip_ids

            for order in customer.get('orders', []):
                oid = order.get('order_id')
                odate = order.get('order_date')
                ship_addr = order.get('shipping_address')
                order_val = order.get('order_total_value')

                for item in order.get('items', []):
                    rows.append({
                        'customer_id': cid,
                        'customer_name': name,
                        'registration_date': reg_date,
                        'is_vip': is_vip,
                        'order_id': oid,
                        'order_date': odate,
                        'shipping_address': ship_addr,
                        'order_total_value': order_val,
                        'product_id': item.get('item_id'),
                        'product_name': item.get('product_name'),
                        'category': item.get('category'),
                        'unit_price': item.get('price'),
                        'item_quantity': item.get('quantity')
                    })
        return pd.DataFrame(rows)

    def transform(self):
        data = self.load_pickle()
        vip_ids = self.load_vip_ids()
        df = self.flatten_data(data, vip_ids)

        # Type conversions
        df['customer_id'] = df['customer_id'].astype(int)
        df['customer_name'] = df['customer_name'].astype(str)
        df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
        df['is_vip'] = df['is_vip'].astype(bool)

        df['order_id'] = pd.to_numeric(df['order_id'], errors='coerce').astype('Int64')
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

        df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce').astype('Int64')
        df['product_name'] = df['product_name'].astype(str)

        # Category mapping
        category_map = {
            1: 'Electronics',
            2: 'Apparel',
            3: 'Books',
            4: 'Home Goods'
        }
        df['category'] = df['category'].map(lambda x: category_map.get(int(x), 'Misc') if pd.notna(x) and str(x).isdigit() else 'Misc')
        df['category'] = df['category'].astype(str)

        df.rename(columns={'unit_price': 'unit_price', 'item_quantity': 'item_quantity'}, inplace=True)
        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
        df['item_quantity'] = pd.to_numeric(df['item_quantity'], errors='coerce').astype('Int64')

        # Derived columns
        df['total_item_price'] = df['unit_price'] * df['item_quantity']
        df['total_order_value_percentage'] = df['total_item_price'] / df['order_total_value'] * 100

        # Sorting
        df.sort_values(by=['customer_id', 'order_id', 'product_id'], ascending=True, inplace=True)

        # Save to CSV
        df.to_csv(self.output_csv, index=False)
        return df
