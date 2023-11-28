from flask import Flask, jsonify, request
from cassandra.cluster import Cluster
from cassandra import InvalidRequest
from functools import wraps
from models import UserDatabase, CustomerDatabase, ProductDatabase, SalesDatabase, SalesPriceDatabase
from utils import validate_customer_data, validate_product_data, validate_sales_data, validate_sales_price_data

app = Flask(__name__)

# Initialize database objects
user_database = UserDatabase()
customer_database = CustomerDatabase()
product_database = ProductDatabase()
sales_database = SalesDatabase()
sales_price_database = SalesPriceDatabase()


# Token authentication decorator
def token_required(f):
    @wraps(f)
    def decorator(*args, **kwargs):
        token = request.headers.get('Authorization')
        username = user_database.validate_token(token)
        if not username:
            return jsonify({'message': 'Token is missing, invalid, or expired'}), 401
        return f(*args, **kwargs)
    return decorator


@app.route('/authenticate', methods=['POST'])
def authenticate():
    username = request.json.get('username')
    password = request.json.get('password')
    if user_database.verify_user(username, password):
        token = user_database.create_token(username)
        return jsonify({'token': token})
    else:
        return jsonify({'message': 'Invalid credentials'}), 401


# Initialize Cassandra Connection
def get_cassandra_session():
    cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra cluster's IP
    session = cluster.connect('mykeyspace')
    return session


# Utility function to convert Cassandra rows to a dictionary
def row_to_dict(row):
    return {column: getattr(row, column) for column in row._fields}

def add_customer_links(customer):
    if customer:
        customer_id = customer["customer_id"]
        customer['links'] = [
            {'rel': 'self', 'href': f'/customers/{customer_id}'},
            {'rel': 'update', 'href': f'/customers/{customer_id}'},
            {'rel': 'delete', 'href': f'/customers/{customer_id}'}
        ]
    return customer

def add_product_links(product):
    if product:
        product_id = product["product_id"]
        product['links'] = [
            {'rel': 'self', 'href': f'/products/{product_id}'},
            {'rel': 'update', 'href': f'/products/{product_id}'},
            {'rel': 'delete', 'href': f'/products/{product_id}'}
        ]
    return product

def add_sale_links(sale):
    if sale:
        sale_id = sale["sale_id"]
        sale['links'] = [
            {'rel': 'self', 'href': f'/sales/{sale_id}'},
            {'rel': 'update', 'href': f'/sales/{sale_id}'},
            {'rel': 'delete', 'href': f'/sales/{sale_id}'}
        ]
    return sale

def add_sales_price_links(sales_price):
    if sales_price:
        product_id = sales_price["product_id"]
        price_date = sales_price["price_date"]
        sales_price['links'] = [
            {'rel': 'self', 'href': f'/salesprices/{product_id}/{price_date}'},
            {'rel': 'update', 'href': f'/salesprices/{product_id}/{price_date}'},
            {'rel': 'delete', 'href': f'/salesprices/{product_id}/{price_date}'}
        ]
    return sales_price


@app.route('/customers', methods=['POST'])
@token_required
def add_customer():
    data = request.json
    is_valid, errors = validate_customer_data(data)
    if not is_valid:
        return jsonify({'error': errors}), 400
    
    customer_id = customer_database.add_customer(data['name'], data['email'], data['phone'], data['address'])
    new_customer = customer_database.get_customer(customer_id)
    return jsonify({'message': 'Customer added successfully', 'customer': add_customer_links(new_customer)}), 201

@app.route('/customers/<uuid:customer_id>', methods=['PUT'])
@token_required
def update_customer(customer_id):
    data = request.json
    is_valid, errors = validate_customer_data(data)
    if not is_valid:
        return jsonify({'error': errors}), 400

    customer_database.update_customer(customer_id, data['name'], data['email'], data['phone'], data['address'])
    updated_customer = customer_database.get_customer(customer_id)
    if updated_customer:
        return jsonify({'message': 'Customer updated successfully', 'customer': add_customer_links(updated_customer)}), 200
    else:
        return jsonify({'error': 'Customer not found'}), 404


@app.route('/customers/<uuid:customer_id>', methods=['DELETE'])
@token_required
def delete_customer(customer_id):
    customer = customer_database.get_customer(customer_id)
    if customer:
        customer_database.delete_customer(customer_id)
        return jsonify({'message': 'Customer deleted successfully'}), 200
    else:
        return jsonify({'error': 'Customer not found'}), 404
    
@app.route('/customers/<uuid:customer_id>', methods=['GET'])
@token_required
def get_customer(customer_id):
    customer = customer_database.get_customer(customer_id)
    if customer:
        return jsonify(add_customer_links(customer))
    else:
        return jsonify({'error': 'Customer not found'}), 404
    
@app.route('/customers', methods=['GET'])
@token_required
def get_all_customers():
    all_customers = customer_database.get_all_customers()  # Assuming this method is implemented in your CustomerDatabase class
    customers_with_links = [add_customer_links(customer) for customer in all_customers]
    return jsonify(customers_with_links)



# Route to add a new product
@app.route('/products', methods=['POST'])
@token_required
def add_product():
    data = request.json
    is_valid, errors = validate_product_data(data)
    if not is_valid:
        return jsonify({'error': errors}), 400

    product_id = product_database.add_product(data['name'], data['description'], data['price'])
    new_product = product_database.get_product(product_id)
    return jsonify({'message': 'Product added successfully', 'product': add_product_links(new_product)}), 201

@app.route('/products/<uuid:product_id>', methods=['GET'])
@token_required
def get_product(product_id):
    product = product_database.get_product(product_id)
    if product:
        return jsonify(add_product_links(product))
    else:
        return jsonify({'error': 'Product not found'}), 404

@app.route('/products', methods=['GET'])
@token_required
def get_all_products():
    all_products = product_database.get_all_products()  # Assuming this method is implemented in your ProductDatabase class
    products_with_links = [add_product_links(product) for product in all_products]
    return jsonify(products_with_links)


# Route to record a sale
@app.route('/sales', methods=['POST'])
@token_required
def record_sale():
    data = request.json
    is_valid, errors = validate_sales_data(data)
    if not is_valid:
        return jsonify({'error': errors}), 400

    sale_id = sales_database.record_sale(data['customer_id'], data['product_id'], data['quantity'], data['total_price'])
    new_sale = sales_database.get_sale(sale_id)
    return jsonify({'message': 'Sale recorded successfully', 'sale': add_sale_links(new_sale)}), 201

@app.route('/sales/<uuid:sale_id>', methods=['GET'])
@token_required
def get_sale(sale_id):
    sale = sales_database.get_sale(sale_id)
    if sale:
        return jsonify(add_sale_links(sale))
    else:
        return jsonify({'error': 'Sale not found'}), 404

@app.route('/sales', methods=['GET'])
@token_required
def get_all_sales():
    all_sales = sales_database.get_all_sales() 
    sales_with_links = [add_sale_links(sale) for sale in all_sales]
    return jsonify(sales_with_links)


# Route to add a sales price
@app.route('/salesprices', methods=['POST'])
@token_required
def add_sales_price():
    data = request.json
    is_valid, errors = validate_sales_price_data(data)
    if not is_valid:
        return jsonify({'error': errors}), 400

    sales_price_database.add_sales_price(data['product_id'], data['sale_price'], data['price_date'], data.get('event'))
    new_sales_price = sales_price_database.get_sales_price(data['product_id'], data['price_date'])
    return jsonify({'message': 'Sales price added successfully', 'sales_price': add_sales_price_links(new_sales_price)}), 201

@app.route('/salesprices/<uuid:product_id>/<price_date>', methods=['GET'])
@token_required
def get_sales_price(product_id, price_date):
    sales_price = sales_price_database.get_sales_price(product_id, price_date)
    if sales_price:
        return jsonify(add_sales_price_links(sales_price))
    else:
        return jsonify({'error': 'Sales price not found'}), 404


# Generic error handler
@app.errorhandler(Exception)
def handle_general_exception(error):
    return jsonify({'error': 'An error occurred', 'details': str(error)}), 500

# Specific handler for Cassandra InvalidRequest
@app.errorhandler(InvalidRequest)
def handle_invalid_request(error):
    return jsonify({'error': 'Database request is invalid', 'details': str(error)}), 400

if __name__ == '__main__':
    app.run(debug=True)
