import ConfigParser
import sys
import datetime
import pymongo
import elasticsearch
import bson
import dateutil.parser
import optparse

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
    def __init__(self, common_timestamp=None,
                 common_field_format=None,
                 sync_field=None,
                 common_index_format=None,
                 common_type_format=None):
        
        self.common_timestamp = common_timestamp
        self.common_field_format = common_field_format
        self.sync_field = sync_field
        self.common_index_format = common_index_format
        self.common_type_format = common_type_format

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

        # Common timestamp field should not exist in document
        assert not doc.has_key(self.common_timestamp), 'Common timestamp field already exists!'

        # Use generation timestamp if available by default.
        if timestamp == None:
            object_id = doc['_id']

            if type(object_id) == bson.objectid.ObjectId:
                doc[self.common_timestamp] = object_id.generation_time

            return                
        
        if not doc.has_key(timestamp):
            return 

        if type(doc[timestamp]) != datetime.datetime:
            if tsformat != None:
                doc[self.common_timestamp] = datetime.datetime.strptime(
                    doc[timestamp], tsformat)
        else:
            doc[self.common_timestamp] = doc[timestamp]

    def add_sync_field_ifset(self, doc):
        if self.sync_field == None:
            return

        # Sync field should not exist in document
        assert not doc.has_key(self.sync_field)

        _id = doc['_id']
        if type(_id) == bson.objectid.ObjectId:
            doc[self.sync_field] = _id.generation_time
        elif self.common_timestamp != None:
            # First run self.add_common_timestamp
            assert doc.has_key(self.common_timestamp)
            doc[self.sync_field] = doc[self.common_timestamp]
            
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
    print 'Usage: ', sys.argv[0], '[--test] [full|sync] config_file'
    sys.exit(1)

# TODO: sync support
    
def main():
    optparser = optparse.OptionParser()
    optparser.add_option('-t', '--test',
                         action='store_true',
                         dest='test',
                         default=False)
    opts, args = optparser.parse_args()
    
    if len(args) != 2:
        usage()

    if args[0] not in ('full', 'sync'):
        usage()        

    config = ConfigParser.RawConfigParser()
    config.read(args[1])

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
        'common_field_format',
        'sync_field',
        'common_index_format',
        'common_type_format'))

    # is this a simulation?
    test = opts.test

    # is this synchronization?
    sync = args[0] == 'sync'

    # Instantiate a simulation
    if test:
        dynamic_mapping = DynamicMappingSimulation()

    # One of those options must be set
    if (sync and filter_config.sync_field == None and
        filter_config.common_timestamp == None):
        
        print '[ERROR] sync_field nor common_timestamp are not set. Cowardly Aborting.'
        return 1

    for index in indices:
        if index.db_name not in mongo_client.database_names():
            print 'Database "%s" not found.' %db_name
            continue
        if index.coll_name not in mongo_client[index.db_name].collection_names():
            print 'Collection "%s" not found.' %'.'.join(index.db_name, index.coll_name)

        db = mongo_client[index.db_name]
        coll = db[index.coll_name]

        cursor = None
        if sync:
            criterion = filter_config.sync_field\
                        if filter_config.sync_field != None\
                        else filter_config.common_timestamp

            # Without try, so it fails in case of RequestError (use --test first)
            result = es.search(
                index=index.index,
                doc_type=index.type,
                body={
                    'sort':[{
                        criterion:{
                            'order':'desc',
                            }
                    }]
                }
            )

            if result != None and result['hits']['total'] > 0:
                last = result['hits']['hits'][0]['_source']
                ts = dateutil.parser.parse(last[criterion])

                '''
                Gets string representation of timestamp if
                tsformat was specified in settings for this
                collections (it is assumed that field
                index.timestamp is a string), else the
                datetime.datetime timestamp is left unchanged
                '''
                get_ts_relative = lambda index, ts: ts.strftime(index.tsformat)\
                                  if index.tsformat != None else ts

                # In case a sync_field is enabled or the timestamp field name
                # was not set for this collection
                if filter_config.sync_field != None or index.timestamp == None:
                    '''
                    Create ObjectId with retrieved timestamp from
                    elasticsearch
                    '''
                    has_objectid = (type(oll.find_one()['_id']) ==
                                    bson.objectid.ObjectId)

                    if has_objectid:
                        relative = bson.objectid.ObjectId.from_datetime(ts)
                        cursor = coll.find({'_id':{'$gt':relative}})
                    elif index.timestamp != None:
                        relative = get_ts_relative(index, ts)
                        cursor = coll.find({index.timestamp:{'$gt':relative}})
                else:
                    '''
                    COMMON_TIMESTAMP is obtained from timestamp
                    in collection
                    '''
                    relative = get_ts_relative(index, ts)
                    cursor = coll.find({index.timestamp:{'$gt':relative}})

        # </if sync> ==> full
        else:
            cursor = coll.find()                

        # Nothing to do
        if cursor == None or cursor.count() == 0:
            print '[EMPTY] Nothing to do for %s.%s' %(index.db_name,
                                              index.coll_name)
            continue
            
        # stats data
        total = cursor.count()
        i = 0
            
        for doc in cursor:
            _id = str(doc['_id'])
            del doc['_id']

            # Add a common timestamp field if set in filters
            filter_config.add_common_timestamp_ifset(doc,
                                                     index.timestamp,
                                                     index.tsformat)

            # Add a sync timestamp field if set in filters
            filter_config.add_sync_field_ifset(doc)
            
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
            
            # Prepare params for elasticsearch index
            params = dict()
            params['index'] = index.index if filter_config.common_index_format == None\
                              else filter_config.common_index_format\
                                   .format(db=index.db_name,
                                           coll=index.coll_name)
            params['doc_type'] = index.type if filter_config.common_type_format == None\
                                 else filter_config.common_type_format\
                                      .format(db=index.db_name,
                                              coll=index.coll_name)
            params['_id'] = _id
            params['body'] = doc

            # If not a test, actually push to ES
            es.index(**params)

            # If not test print progress after indexing
            print_progress(index.db_name, index.coll_name, i, total)

        # </for doc in coll.find()>
        print

    if test:
        print 'Test passed succesfully.'
                

if __name__ == '__main__':
    main()
