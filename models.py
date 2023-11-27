class Item:
    def __init__(self, id, name, description):
        self.id = id
        self.name = name
        self.description = description

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description
        }

class ItemDatabase:
    def __init__(self):
        self.items = []
        self.next_id = 1

    def add_item(self, name, description):
        item = Item(self.next_id, name, description)
        self.items.append(item)
        self.next_id += 1
        return item

    def get_item(self, id):
        return next((item for item in self.items if item.id == id), None)

    def update_item(self, id, name, description):
        item = self.get_item(id)
        if item:
            item.name = name
            item.description = description
        return item

    def delete_item(self, id):
        self.items = [item for item in self.items if item.id != id]

# Instantiate the database
item_database = ItemDatabase()
