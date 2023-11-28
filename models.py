from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import bcrypt
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
import uuid
import hashlib
from utils import row_to_dict


def generate_token():
    return str(uuid.uuid4())


class UserDatabase:
    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra cluster IPs
        self.session = self.cluster.connect('mykeyspace')

    def create_user(self, username, password):
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        query = "INSERT INTO userinfo (username, password) VALUES (%s, %s)"
        self.session.execute(query, (username, hashed.decode('utf-8')))

    def verify_user(self, username, password):
        query = "SELECT password FROM userinfo WHERE username = %s"
        user = self.session.execute(query, (username,)).one()
        if user:
            return bcrypt.checkpw(password.encode('utf-8'), user.password.encode('utf-8'))
        return False

    def authenticate_user(self, username, password):
        if self.verify_user(username, password):
            token = str(uuid.uuid4())
            return token
        return None

    def create_token(self, username):
        token = str(uuid.uuid4())
        expiry = datetime.now() + timedelta(days=14)  # Token valid for 14 days
        query = "INSERT INTO user_tokens (username, token, expiry) VALUES (%s, %s, %s)"
        self.session.execute(query, (username, token, expiry))
        return token

    def validate_token(self, token):
        query = "SELECT username, expiry FROM user_tokens WHERE token = %s"
        result = self.session.execute(query, (token,)).one()
        if result and result.expiry > datetime.now():
            return result.username
        return None

    def update_user(self, username, new_password):
        hashed = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
        query = "UPDATE userinfo SET password = %s WHERE username = %s"
        self.session.execute(query, (hashed.decode('utf-8'), username))

    def delete_user(self, username):
        query = "DELETE FROM userinfo WHERE username = %s"
        self.session.execute(query, (username,))

    def get_all_users(self):
        query = "SELECT * FROM userinfo"
        return [row_to_dict(row) for row in self.session.execute(query)]

    
class CustomerDatabase:
        def __init__(self):
            self.cluster = Cluster(['127.0.0.1'])
            self.session = self.cluster.connect('mykeyspace')

        def add_customer(self, name, email, phone, address):
            customer_id = uuid.uuid4()
            query = "INSERT INTO customers (customer_id, name, email, phone, address) VALUES (%s, %s, %s, %s, %s)"
            self.session.execute(query, (customer_id, name, email, phone, address))
            return customer_id

        def get_customer(self, customer_id):
            query = "SELECT * FROM customers WHERE customer_id = %s"
            return self.session.execute(query, (customer_id,)).one()

        def delete_customer(self, customer_id):
            query = "DELETE FROM customers WHERE customer_id = %s"
            self.session.execute(query, (customer_id,))

        def update_customer(self, customer_id, name, email, phone, address):
            query = "UPDATE customers SET name = %s, email = %s, phone = %s, address = %s WHERE customer_id = %s"
            self.session.execute(query, (name, email, phone, address, customer_id))

        def get_all_customers(self):
            query = "SELECT * FROM customers"
            return [row_to_dict(row) for row in self.session.execute(query)]
        

class ProductDatabase:
    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect('mykeyspace')

    def add_product(self, name, description, price):
        product_id = uuid.uuid4()
        query = "INSERT INTO products (product_id, name, description, price) VALUES (%s, %s, %s, %s)"
        self.session.execute(query, (product_id, name, description, price))
        return product_id

    def get_product(self, product_id):
        query = "SELECT * FROM products WHERE product_id = %s"
        return self.session.execute(query, (product_id,)).one()
    
    def delete_product(self, product_id):
        query = "DELETE FROM products WHERE product_id = %s"
        self.session.execute(query, (product_id,))
    
    def update_product(self, product_id, name, description, price):
        query = "UPDATE products SET name = %s, description = %s, price = %s WHERE product_id = %s"
        self.session.execute(query, (name, description, price, product_id))

    def get_all_products(self):
        query = "SELECT * FROM products"
        return [row_to_dict(row) for row in self.session.execute(query)]

class SalesDatabase:
    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect('mykeyspace')

    def record_sale(self, customer_id, product_id, quantity, total_price):
        sale_id = uuid.uuid4()
        sale_date = datetime.now()
        query = "INSERT INTO sales (sale_id, customer_id, product_id, quantity, sale_date, total_price) VALUES (%s, %s, %s, %s, %s, %s)"
        self.session.execute(query, (sale_id, customer_id, product_id, quantity, sale_date, total_price))
        return sale_id

    def get_sale(self, sale_id):
        query = "SELECT * FROM sales WHERE sale_id = %s"
        return self.session.execute(query, (sale_id,)).one()
    
    def delete_sale(self, sale_id):
        query = "DELETE FROM sales WHERE sale_id = %s"
        self.session.execute(query, (sale_id,))
    
    def update_sale(self, sale_id, customer_id, product_id, quantity, total_price):
        query = "UPDATE sales SET customer_id = %s, product_id = %s, quantity = %s, total_price = %s WHERE sale_id = %s"
        self.session.execute(query, (customer_id, product_id, quantity, total_price, sale_id))

    def get_all_sales(self):
        query = "SELECT * FROM sales"
        return [row_to_dict(row) for row in self.session.execute(query)]

class SalesPriceDatabase:
    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect('mykeyspace')

    def calculate_checksum(self, data):
        checksum = hashlib.sha256(data.encode()).hexdigest()
        return checksum

    def add_sales_price(self, product_id, sale_price, price_date, event=None):
        # Convert inputs to string and concatenate for checksum calculation
        data_str = f"{product_id}-{sale_price}-{price_date}-{event}"
        checksum = self.calculate_checksum(data_str)

        query = "INSERT INTO salesprices (product_id, sale_price, price_date, event, checksum) VALUES (%s, %s, %s, %s, %s)"
        self.session.execute(query, (product_id, sale_price, price_date, event, checksum))

    def get_sales_price(self, product_id, price_date):
        query = "SELECT * FROM salesprices WHERE product_id = %s AND price_date = %s"
        record = self.session.execute(query, (product_id, price_date)).one()
        if record:
            # Re-calculate checksum
            data_str = f"{record.product_id}-{record.sale_price}-{record.price_date}-{record.event}"
            checksum = self.calculate_checksum(data_str)

            if checksum != record.checksum:
                raise ValueError("Data integrity check failed for sales price record")
        return record
    
    def update_sales_price(self, product_id, price_date, sale_price, event):
        # Recalculate checksum for the updated data
        data_str = f"{product_id}-{sale_price}-{price_date}-{event}"
        checksum = self.calculate_checksum(data_str)

        query = "UPDATE salesprices SET sale_price = %s, event = %s, checksum = %s WHERE product_id = %s AND price_date = %s"
        self.session.execute(query, (sale_price, event, checksum, product_id, price_date))

    def delete_sales_price(self, product_id, price_date):
        query = "DELETE FROM salesprices WHERE product_id = %s AND price_date = %s"
        self.session.execute(query, (product_id, price_date))
