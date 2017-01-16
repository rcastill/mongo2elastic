import pymongo

class MongoConfig(object):
    def __init__(self):
        # TODO: Auth support
        client = pymongo.MongoClient()
