import mongo2elastic as m2e

import datetime as dt


class Index(object):
    def __init__(self):
        self.index = '__db__'
        self.type = '__coll__'
        self.doc = '__doc__'

class Asdms365(m2e.Collection):
    def __init__(self):
        m2e.Collection.__init__(self, 'asdms365')

    def filter(self, doc):
        doc['date'] = dt.datetime.strftime(doc['date'],
                                           '%Y-%m')

        
class Ducksdev(m2e.DB):
    def __init__(self):
        m2e.DB.__init__(self, 'ducksdev')

        self.add_collection('asdms')
        self.add_collection(Asdms365)
        
        
        self.hook_common_filter(self.filter_all,
                                'asdms')

    def filter_all(self, coll, db):
        keys = index.doc.keys()
        for key in keys:
            new_key = '%s_%s__%s'\
                      %(coll,
                        key,
                        type2str(type(index.doc[key])))
            index.doc[new_key] = index.doc[key]
            del index.doc[key]



def main():
    es = m2e.elastic.ElasticConfig()
    mongo = m2e.mongo.MongoConfig()
    indexer = m2e.Indexer(es, mongo)
    indexer.register_db(Ducksdev)
    indexer.index(sys.argv)
