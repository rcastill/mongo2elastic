import elasticsearch as es

class ElasticConfig(object):
    def __init__(self, user='elastic',
                 password='changeme',
                 uri='localhost',
                 port='9200'):

        full_uri = '%s:%s@%s:%s' %(user,
                                   password,
                                   uri,
                                   port)
        self.client = es.Elasticsearch(full_uri)
