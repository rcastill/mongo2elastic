import collection

class CommonFilter(object):
    def __init__(self):
        self._collections = set()

    def add_collection(self, coll):
        assert type(coll) == collection.Collection
        self.collections.add(coll)

    def filter(self, doc):
        pass

    
