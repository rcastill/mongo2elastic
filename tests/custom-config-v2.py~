import ConfigParser
import sys
import datetime
import pymongo

class IndexConfig(object):
    def __init__(self, db_name, coll_name,
                 timestamp=None,
                 tsformat=None,
                 index=None,
                 type=None):

        self.db_name = db_name
        self.coll_name = coll_name
        self.timestamp = timestamp
        self.tsformat = tsformat
        self.index = db_name if index == None else index
        self.type = coll_name if type == None else _type

class ElasticsearchConfig(object):
    default_user = ''
    default_password = ''
    default_uri = 'localhost'
    default_port = '9200'
    
    def __init__(self, user='', password='', uri='localhost', port='9200'):
        self.user = user
        self.password = password
        self.uri = uri
        self.port = port

    def get_uri(self):
        base = 'http://{0}'+self.uri+':'+self.port
        if self.user != '' and self.password != '':
            return base.format('{0}:{1}@'.format(self.user, self.password))
        else:
            return base.format('')

class FilterConfig(object):
    def __init__(self, common_timestamp=None, common_field_format=None):
        self.common_timestamp = common_timestamp
        self.common_field_format = common_field_format

    def is_default(self):
        return self.common_field_format == None

    def filter_fields(self, doc, db_name, coll_name):
        keys = doc.keys()
        for key in keys:
            if key == self.common_timestamp:
                continue
            
            new_key = self.common_field_format\
                      .format(field=key,
                              coll=coll_name,
                              type=type2str(type(doc[key])),
                              db=db_name)

            # If there is an original key with generated name, FAIL!
            assert not doc.has_key(new_key) # first should check if default!

            doc[new_key] = doc[key]
            del doc[key]

        return doc

    def add_common_timestamp_ifset(self, doc, timestamp, tsformat=None):
        if self.common_timestamp == None:
            return

        if not doc.has_key(timestamp):
            return

        assert not doc.has_key(self.common_timestamp)

        if type(doc[timestamp]) != datetime.datetime:
            if tsformat != None:
                try:
                    doc[self.common_timestamp] = datetime.datetime.strptime(
                        doc[timestamp], tsformat)
                except ValueError:
                    pass
        else:
            doc[self.common_timestamp] = doc[timestamp]
            
def make_params(config, section, *param_names):
    params = dict()
    for param_name in param_names:
        if config.has_option(section, param_name):
            params[param_name] = config.get(section, param_name)
    return params

def type2str(t):
    return str(t).replace("<type '", '').replace("'>", '')

def main():
    assert(len(sys.argv) == 2)
    config = ConfigParser.RawConfigParser()
    config.read(sys.argv[1])

    mongo_client = pymongo.MongoClient()

    # Get (db_namef, coll_name) pairs
    targets = [tuple(target.split(':')[1].split('.'))
               for target in config.sections()
               if target.startswith('index:')]

    # Get collections to index
    indices = []
    for target in targets:
        section = 'index:%s.%s' %target
        params = make_params(config,
                             section,
                             'timestamp',
                             'index',
                             'type',
                             'tsformat')
        indices.append(IndexConfig(*target,
                                   **params))

    # Get filter config
    filter_config = FilterConfig(**make_params(
        config,
        'filter',
        'common_timestamp',
        'common_field_format'))

    for index in indices:
        if index.db_name not in mongo_client.database_names():
            print 'Database "%s" not found.' %db_name
            continue
        if index.coll_name not in mongo_client[index.db_name].collection_names():
            print 'Collection "%s" not found.' %'.'.join(index.db_name, index.coll_name)

        db = mongo_client[index.db_name]
        coll = db[index.coll_name]

        for doc in coll.find():
            _id = str(doc['_id'])
            del doc['_id']

            # Add a common timestamp field if set in filters
            filter_config.add_common_timestamp_ifset(doc,
                                                     index.timestamp,
                                                     index.tsformat)
            
            if not filter_config.is_default():
                filter_config.filter_fields(doc,
                                            index.db_name,
                                            index.coll_name)
            
            print doc

if __name__ == '__main__':
    main()
