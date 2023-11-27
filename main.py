from flask import Flask, jsonify, request
from models import item_database 
from utils import validate_item_data

app = Flask(__name__)

# Route to get all items
@app.route('/items', methods=['GET'])
def get_items():
    # Convert all items to dictionaries and add HATEOAS links
    all_items = [item.to_dict() for item in item_database.items]
    return jsonify([add_links(item) for item in all_items])

# Route to get a single item by its ID
@app.route('/items/<int:item_id>', methods=['GET'])
def get_item(item_id):
    # Find the item by ID and return it if found
    item = item_database.get_item(item_id)
    if item:
        return jsonify(add_links(item.to_dict()))
    else:
        # Return a 404 error if the item is not found
        return jsonify({'error': 'Item not found'}), 404

# Route to add a new item
@app.route('/items', methods=['POST'])
def add_item():
    data = request.json
    # Validate incoming data
    if not validate_item_data(data):
        # Return a 400 error if validation fails
        return jsonify({'error': 'Missing required fields'}), 400
    # Create a new item and return it
    new_item = item_database.add_item(data['name'], data['description'])
    return jsonify(add_links(new_item.to_dict())), 201

# Route to update an existing item
@app.route('/items/<int:item_id>', methods=['PUT'])
def update_item(item_id):
    data = request.json
    # Update the item if it exists
    updated_item = item_database.update_item(item_id, data['name'], data['description'])
    if updated_item:
        return jsonify(add_links(updated_item.to_dict()))
    else:
        # Return a 404 error if the item to update is not found
        return jsonify({'error': 'Item not found'}), 404

# Route to delete an item
@app.route('/items/<int:item_id>', methods=['DELETE'])
def delete_item(item_id):
    # Delete the item if it exists
    if item_database.get_item(item_id):
        item_database.delete_item(item_id)
        return '', 204
    else:
        # Return a 404 error if the item to delete is not found
        return jsonify({'error': 'Item not found'}), 404

# Function to add HATEOAS links to an item
def add_links(item):
    # Add hypermedia links to the item
    if item:
        item['links'] = [
            {'rel': 'self', 'href': f'/items/{item["id"]}'},
            {'rel': 'update', 'href': f'/items/{item["id"]}'},
            {'rel': 'delete', 'href': f'/items/{item["id"]}'}
        ]
    return item

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)
