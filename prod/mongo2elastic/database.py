import collection

class Database(object):
    def __init__(self, name):
        self._name = name
        self._collections = []

    def add_collection(self, coll):
        assert type(coll) == collection.Collection

        self._collections.append(coll)
            
