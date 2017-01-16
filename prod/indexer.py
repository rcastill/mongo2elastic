import ConfigParser
import sys
import datetime
import pymongo
import elasticsearch
import bson
import dateutil.parser
import optparse

def objectid_counter(objectid):
    # objectid: bson.objectid.ObjectId
    last3bytes = str(objectid)[-6:]
    return int('0x'+last3bytes, 16)

class IndexConfig(object):
    def __init__(self, db_name, coll_name,
                 timestamp=None,
                 tsformat=None,
                 index=None,
                 type=None,
                 script=None):

        self.db_name = db_name
        self.coll_name = coll_name
        self.timestamp = timestamp
        self.tsformat = tsformat
        self.index = db_name if index == None else index
        self.type = coll_name if type == None else type

        if script == None:
            self.filter_fn = lambda doc: None
        else:
            mod = __import__('filters.%s' %script)
            sub = getattr(mod, script)
            self.filter_fn = sub.filter

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
                 sync_field_inc_suffix='__INC',
                 common_index_format=None,
                 common_type_format=None):
        
        self.common_timestamp = common_timestamp
        self.common_field_format = common_field_format
        self.sync_field = sync_field
        self.sync_field_inc_suffix = sync_field_inc_suffix
        self.common_index_format = common_index_format
        self.common_type_format = common_type_format

    def is_default(self):
        return self.common_field_format == None

    def sync_inc_field(self):
        return self.sync_field + self.sync_field_inc_suffix
    
    def get_index_name(self, index):
        if self.common_index_format == None:
            return index.index
        # if index name is not default, prefer custom
        elif index.index == index.db_name:
            return self.common_index_format.format(db=index.db_name,
                                                   coll=index.coll_name)

    def get_type_name(self, index):
        if self.common_type_format == None:
            return index.type
        # if type name is not default, prefer custom
        elif index.type == index.coll_name:
            return self.common_type_format.format(db=index.db_name,
                                                  coll=index.coll_name)        

    def filter_fields(self, doc, db_name, coll_name):
        keys = doc.keys()
        for key in keys:
            if key == self.common_timestamp or\
               key == self.sync_field or\
               key == self.sync_inc_field():
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

    def add_sync_field_ifset(self, doc, _id):
        if self.sync_field == None:
            return

        # Sync field should not exist in document
        assert not doc.has_key(self.sync_field)

        if type(_id) == bson.objectid.ObjectId:
            doc[self.sync_field] = _id.generation_time
            doc[self.sync_inc_field()] = objectid_counter(_id)
            #doc[self.sync_inc_field()] = str(_id)

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
    if t == datetime.datetime:
        return 'datetime'
    
    return str(t).replace("<type '", '').replace("'>", '')

def print_title():
    title = '{0: <9} {1: <40} {2: <40} {3: <20} {4: >20}'.format('%',
                                                                 'DB.COLLECTION',
                                                                 'INDEX/TYPE',
                                                                 'DOCS',
                                                                 'STATUS')
    print title
    print '=' * len(title)

def print_progress(index, filter_config, i, total, status):
    prog_per = str('{0:.2f}'.format(float(i)/total*100))
    prog_raw = str(i) + '/' + str(total)
    from_txt = index.db_name + '.' + index.coll_name
    dest_txt = filter_config.get_index_name(index) + '/' +\
               filter_config.get_type_name(index)
    
    sys.stdout.write('[{0: >6}%] {1: <40} {2: <40} {3: <20} {4: >20}\r'.format(prog_per,
                                                                            from_txt,
                                                                            dest_txt,
                                                                            prog_raw,
                                                                            status))
    
    sys.stdout.flush()
    
def usage():
    print 'Usage: ', sys.argv[0], '[--test] [full|sync|update] config_file'
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

    if args[0] not in ('full', 'sync', 'update'):
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
                             'tsformat',
                             'script')
        indices.append(IndexConfig(*target,
                                   **params))

    # Get filter config
    filter_config = FilterConfig(**make_params(
        config,
        'filter',
        'common_timestamp',
        'common_field_format',
        'sync_field',
        'sync_field_inc_suffix',
        'common_index_format',
        'common_type_format'))

    # is this a simulation?
    test = opts.test

    # is this synchronization?
    sync = args[0] == 'sync'

    # is this update?
    update = args[0] == 'update'

    # Instantiate a simulation
    if test:
        dynamic_mapping = DynamicMappingSimulation()

    # One of those options must be set
    if (sync and filter_config.sync_field == None and
        filter_config.common_timestamp == None):
        
        print '[ERROR] sync_field nor common_timestamp are not set. Cowardly Aborting.'
        return 1

    if update:
        update_counters = {}

    # Aesthetics
    print_title()

    # Print nothing to do in the end
    nothing_to_do = []
    
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
            sort_body = { 'sort':[] }

            if filter_config.sync_field != None:
                criterion = filter_config.sync_field # almost legacy
                sort_body['sort'].extend([
                    {
                        filter_config.sync_field: { 'order': 'desc' }
                    },
                    {
                        filter_config.sync_inc_field(): { 'order': 'desc' }
                    }
                ])
            else:
                criterion = filter_config.common_timestamp # almost legacy
                sort_body['sort'].append(
                    {
                        filter_config.common_timestamp: { 'order': 'desc' }
                    })
                    
            # Without try, so it fails in case of RequestError (use --test first)
            result = es.search(
                index=filter_config.get_index_name(index),
                doc_type=filter_config.get_type_name(index),
                body=sort_body
            )

            if result != None and result['hits']['total'] > 0:
                last = result['hits']['hits'][0]
                ts = dateutil.parser.parse(last['_source'][criterion])

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
                    has_objectid = (type(coll.find_one()['_id']) ==
                                    bson.objectid.ObjectId)

                    if has_objectid:
                        # Try to fully recover object id
                        if bson.objectid.ObjectId.is_valid(last['_id']):
                            relative = bson.objectid.ObjectId(last['_id'])
                        # Recover from datetime
                        else:
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

        # </if sync> ==> full/update
        else:
            cursor = coll.find()                

        # Nothing to do
        if cursor == None or cursor.count() == 0:
            nothing_to_do.append(index)
            continue
            
        # stats data
        total = cursor.count()
        i = 0
            
        for doc in cursor:
            object_id = doc['_id']
            _id = str(object_id)
            del doc['_id']

            index.filter_fn(doc)

            # Add a common timestamp field if set in filters
            filter_config.add_common_timestamp_ifset(doc,
                                                     index.timestamp,
                                                     index.tsformat)

            # Add a sync timestamp field if set in filters
            filter_config.add_sync_field_ifset(doc, object_id)
            
            if not filter_config.is_default():
                filter_config.filter_fields(doc,
                                            index.db_name,
                                            index.coll_name)

            # Stat update
            i += 1
                
            if update:
                if test:
                    print_progress(index, filter_config, i, total, 'CHECKING FOR UPDATES')
                else:
                    print_progress(index, filter_config, i, total, 'UPDATING')
                
                cindex = filter_config.get_index_name(index)
                ctype = filter_config.get_type_name(index)
                joined = '%s.%s' %(cindex, ctype)

                if not update_counters.has_key(joined):
                    update_counters[joined] = 0
                
                would_update = False
                
                try:
                    found = es.get(index=cindex,
                                   doc_type=ctype,
                                   id=_id)

                    if test:
                        # naive criterion
                        would_update = len(found['_source'].keys()) != len(doc.keys())
                        update_counters[joined] += 1 if would_update else 0
                        continue
                    
                    # Let elasticsearch merge
                    result = es.update(index=index.index,
                                       doc_type=index.type,
                                       id=_id,
                                       body={'doc':doc})

                    update_counters += 1 if result['result'] == 'updated' else 0
                        
                    
                except elasticsearch.TransportError as e:
                    # element not found - insert
                    if e.status_code == 404:
                        # either case increment counter (test or update)
                        update_counters[joined] += 1

                        if test:
                            continue
                        
                        es.index(index=index.index,
                                 doc_type=index.type,
                                 id=_id,
                                 body=doc)

                continue
                                                
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
                print_progress(index, filter_config, i, total, 'CHECKING')
                continue
            
            # If not a test, actually push to ES
            es.index(index=filter_config.get_index_name(index),
                     doc_type=filter_config.get_type_name(index),
                     id=_id,
                     body=doc)

            # If not test print progress after indexing
            print_progress(index, filter_config, i, total, 'INDEXING')

        # </for doc in coll.find()>
        if test:
            if update:
                # joined is in if update: ^
                if update_counters[joined] > 0:
                    print_progress(index,
                                   filter_config,
                                   i,
                                   total,
                                   'BEHIND BY %d DOCS' % update_counters[joined])
                else:
                    print_progress(index, filter_config, i, total, 'UP TO DATE')
            else:
                print_progress(index, filter_config, i, total, 'OK')
        else:
            if update:
                if update_counters[joined] > 0:
                    print_progress(index,
                                   filter_config,
                                   i,
                                   total,
                                   '%d DOCS UPDATED' %update_counters[joined])
                else:
                    print_progress(index,
                                   filter_config,
                                   i,
                                   total,
                                   'UP TO DATE')
            else:
                print_progress(index, filter_config, i, total, 'INDEXED')
        print

    print
    for index in nothing_to_do:
        print '[ ! ] Nothing to do for %s.%s' %(index.db_name,
                                                    index.coll_name)

    if test:
        print '\nTest passed succesfully.'

                

if __name__ == '__main__':
    main()
