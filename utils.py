def validate_item_data(data):
    required_fields = ['name', 'description']
    return all(field in data for field in required_fields)