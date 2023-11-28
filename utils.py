def row_to_dict(row):
    return {column: getattr(row, column) for column in row._fields}


def validate_customer_data(data):
    errors = {}
    required_fields = ['name', 'email', 'phone', 'address']

    for field in required_fields:
        if field not in data:
            errors[field] = 'This field is required.'
        elif not isinstance(data[field], str):
            errors[field] = 'Invalid data type.'

    return len(errors) == 0, errors

def validate_product_data(data):
    errors = {}
    required_fields = ['name', 'description', 'price']

    for field in required_fields:
        if field not in data:
            errors[field] = 'This field is required.'
        elif field == 'price' and not isinstance(data[field], (int, float)):
            errors[field] = 'Invalid data type for price.'
        elif field != 'price' and not isinstance(data[field], str):
            errors[field] = 'Invalid data type.'

    return len(errors) == 0, errors

def validate_sales_data(data):
    errors = {}
    required_fields = ['customer_id', 'product_id', 'quantity', 'total_price']

    for field in required_fields:
        if field not in data:
            errors[field] = 'This field is required.'
        elif field in ['quantity', 'total_price'] and not isinstance(data[field], (int, float)):
            errors[field] = 'Invalid data type for ' + field + '.'
        elif field in ['customer_id', 'product_id'] and not isinstance(data[field], str):
            errors[field] = 'Invalid data type for ' + field + '.'

    return len(errors) == 0, errors

def validate_sales_price_data(data):
    errors = {}
    required_fields = ['product_id', 'sale_price', 'price_date']

    for field in required_fields:
        if field not in data:
            errors[field] = 'This field is required.'
        elif field == 'sale_price' and not isinstance(data[field], (int, float)):
            errors[field] = 'Invalid data type for sale_price.'
        elif field in ['product_id', 'price_date'] and not isinstance(data[field], str):
            errors[field] = 'Invalid data type for ' + field + '.'

    return len(errors) == 0, errors
