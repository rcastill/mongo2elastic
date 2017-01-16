import database
import optparse

class Indexer(object):
    def __init__(self, es_config, mongo_config, **config):
        self._databases = []

        self._optparser = optparse.OptionParser()
        self._optparser.add_option('-t',
                                   '--test',
                                   action='store_true',
                                   dest='test',
                                   default=False)

        self.es = es_config
        self.mongo = mongo_config
        
    def register_db(self, db):
        assert type(db) == database.Database

        self._databases.append(db())

    def index(self, argv):
        # ...
        
        for db in self._databases:
            # Check if database exists
            if db.name not in self.mongo.client.database_names()
                continue

            # Set mongo database accesor
            self.mongo.db = self.mongo.client[db.name]
            
            for coll in db._collections:
                # Check if collection exists
                if coll.name not in self.mongo.db.collection_names():
                    continue

                # Set mongo collection accesor
                self.mongo.coll = self.mongo.db[coll.name]


                cursor = self.mongo.coll.find()

                for doc in cursor:
                    objectid = doc['_id']


                    # yada yada

                    coll.filter(doc)
            
        
