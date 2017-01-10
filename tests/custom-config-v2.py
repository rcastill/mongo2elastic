import ConfigParser
import sys
import datetime
import pymongo
import elasticsearch

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
            assert not doc.has_key(new_key), 'Generated key already exists: %s' %new_key

            doc[new_key] = doc[key]
            del doc[key]

        return doc

    def add_common_timestamp_ifset(self, doc, timestamp, tsformat=None):
        if self.common_timestamp == None:
            return

        if not doc.has_key(timestamp):
            return

        assert not doc.has_key(self.common_timestamp), 'Common timestamp field already exists!' 

        if type(doc[timestamp]) != datetime.datetime:
            if tsformat != None:
                doc[self.common_timestamp] = datetime.datetime.strptime(
                    doc[timestamp], tsformat)
        else:
            doc[self.common_timestamp] = doc[timestamp]

class DynamicMappingSimulation(object):
    class Index(object):
        def __init__(self):
            self.fields = dict()

    class Field(object):
        def __init__(self, _type, index_config):
            self.type = _type
            self.index_config = index_config

    class ConflictingField(object):
        def __init__(self, name, field):
            self.name = name
            self.db_name = field.index_config.db_name
            self.coll_name = field.index_config.coll_name
            
    def __init__(self):
        self.indices = dict()

    def test_doc(self, index_config, doc):
        index_name = index_config.index
        # If index does not exist, create it and pass test
        if not self.indices.has_key(index_name):
            self.indices[index_name] = DynamicMappingSimulation.Index()
            index = self.indices[index_name]

            for key in doc.keys():
                index.fields[key] = DynamicMappingSimulation.Field(type(doc[key]), index_config)

            return None

        # If index existed, check that types are consistent
        index = self.indices[index_name]
        for key in doc.keys():
            if index.fields.has_key(key):
                if index.fields[key].type != type(doc[key]):
                    field = index.fields[key]
                    return DynamicMappingSimulation.ConflictingField(key, field)

                continue

            index.fields[key] = DynamicMappingSimulation.Field(type(doc[key]), index_config)

        # Index existed and types were consistent
        return None
            
def make_params(config, section, *param_names):
    params = dict()
    for param_name in param_names:
        if config.has_option(section, param_name):
            params[param_name] = config.get(section, param_name)
    return params

def type2str(t):
    return str(t).replace("<type '", '').replace("'>", '')

def print_progress(db_name, coll_name, i, total):
    print '{0}\r'.format(
        str('[{0:.2f}'.format(float(i)/total*100)) + '%] ' +
        db_name + '.' + coll_name + ' ' + str(i) + '/'
        + str(total)),
    sys.stdout.flush()
    
def usage():
    print 'Usage: ', sys.argv[0], '[test|full|sync] config_file'
    sys.exit(1)

# TODO: sync support
    
def main():
    if len(sys.argv) != 3:
        usage()

    if sys.argv[1] not in ('test', 'full', 'sync'):
        usage()        

    config = ConfigParser.RawConfigParser()
    config.read(sys.argv[2])

    es_config = ElasticsearchConfig(**make_params(
        config,
        'elasticsearch',
        'user',
        'password',
        'uri',
        'port'))\
        if 'elasticsearch' in config.sections()\
        else ElasticsearchConfig()

    # Initialize Mongo client
    mongo_client = pymongo.MongoClient()

    # Initialize elasticsearch client
    es = elasticsearch.Elasticsearch(es_config.get_uri())

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

    # is this a simulation?
    test = sys.argv[1] == 'test'

    # Instantiate a simulation
    if test:
        dynamic_mapping = DynamicMappingSimulation()

    for index in indices:
        if index.db_name not in mongo_client.database_names():
            print 'Database "%s" not found.' %db_name
            continue
        if index.coll_name not in mongo_client[index.db_name].collection_names():
            print 'Collection "%s" not found.' %'.'.join(index.db_name, index.coll_name)

        db = mongo_client[index.db_name]
        coll = db[index.coll_name]

        # stats data
        total = coll.find().count()
        i = 0
        
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

            # Stat update
            i += 1
                
            if test:
                # Check dynamic mapping simulation
                conflicting_field = dynamic_mapping.test_doc(index,
                                                             doc)

                if conflicting_field != None:
                    # pretty_field(db_name, coll_name, field)
                    pretty_field = lambda d, c, f: '%s:%s[%s]' %(d, c, f)
                    
                    print 'Dynamic Mapping test failed:'
                    print '\tOriginal field:', pretty_field(conflicting_field.db_name,
                                                            conflicting_field.coll_name,
                                                            conflicting_field.name)
                    print '\tConflicting field:', pretty_field(index.db_name,
                                                               index.coll_name,
                                                               conflicting_field.name)
                    return
                
                # If test, print progress here
                print_progress(index.db_name, index.coll_name, i, total)
                continue

            # If not a test, actually push to ES
            es.index(index=index.index,
                     doc_type=index.type,
                     id=_id,
                     body = doc)

            # If not test print progress after indexing
            print_progress(index.db_name, index.coll_name, i, total)

        # </for doc in coll.find()>
        print

    if test:
        print 'Test passed succesfully.'
                

if __name__ == '__main__':
    main()
