import elasticsearch
import pymongo
import json
import sys
import datetime

class InvalidSyntax(Exception):
    pass

class CollectionDoesNotExist(Exception):
    pass

class ParseResult(object):
    def __init__(self, *args):
        if len(args) == 2:
            self.db = args[0]
            self.collections = args[1]
            self.ex = None
        elif len(args) == 1:
            self.db = None
            self.collections = None
            self.ex = args[0]
        else:
            self.db = None
            self.collections = None
            self.ex = None

    def is_valid(self):
        return\
            self.db != None and\
            self.collections != None and\
            self.ex == None
        

def parse(mongo_client, path):
    with open(path) as f:
        # Each line has the format
        # db:(+|-){collection[,]}
        for i, line in enumerate(f):
            split = line.split(':')

            # It must have a db and details section
            if len(split) != 2:
                raise InvalidSyntax("At line %d", i + 1)

            # DB must exist
            dbname = split[0]
            if dbname not in mongo_client.database_names():
                yield ParseResult(InvalidSyntax("DB cannot be empty"))
            db = mongo_client[dbname]

            # Details should at least contain instruction
            details = split[1]
            if len(details) == 0:
                yield ParseResult(InvalidSyntax("Details cannot be empty"))

            # Instruction must be + or -
            instr = details[0]
            if instr not in ('+', '-'):
                 yield ParseResult(InvalidSyntax("Instruction must be +|-"))

            # Get collection names in details
            collections = details[1:].strip().split(',')
            # Get collections in database
            collection_names = db.collection_names()

            # Check if selected collections exist
            if instr == '+':
                for collection in collections:
                    if collection not in collection_names:
                        yield ParseResult(CollectionDoesNotExist(collection))
                yield ParseResult(db, collections)
            # Blacklist detailed collections
            else:
                yield ParseResult(db, list(
                    set(collection_names) -
                    set(collections)))

def type2str(t):
    return str(t).replace("<type '", '').replace("'>", '')
                
def usage():
    print 'Usage: ', sys.argv[0], 'config_file'
    sys.exit(1)

def main():
    if len(sys.argv) != 2:
        usage()
    
    config_fname = sys.argv[1]
    mongo_client = pymongo.MongoClient()

    es_access = 'http://elastic:changeme@localhost:9200'
    es = elasticsearch.Elasticsearch(es_access)

    for parse_result in parse(mongo_client, config_fname):
        if parse_result.ex:
            print parse_result.ex.message
            continue

        if not parse_result.is_valid():
            print "Parse result is not valid!"
            continue
        
        db = parse_result.db
        for collection in parse_result.collections:

            total = db[collection].find().count()
            i = 0

            for doc in db[collection].find():
                _id = str(doc['_id'])
                del doc['_id']

                keys = doc.keys()
                for key in keys:
                    diff = ''
                    while True:
                        new_key = collection + '_' +\
                                  key + '__' +\
                                  type2str(type(doc[key])) +\
                                  str(diff)

                        if doc.has_key(new_key):
                            if diff == '':
                                diff = 0
                            else:
                                diff += 1
                            continue

                        doc[new_key] = doc[key]
                        del doc[key]

                        break
                            
                es.index(index=db.name,
                         doc_type=collection,
                         id=_id,
                         body=doc)

                i += 1
                print '{0}\r'.format(
                    str('[{0:.2f}'.format(float(i)/total*100)) + '%] ' +
                    db.name + '.' + collection + ' ' + str(i) + '/'
                    + str(total)),
                sys.stdout.flush()
            print

        
    """
    es_access = 'http://elastic:changeme@localhost:9200'
    es = elasticsearch.Elasticsearch(es_access)
    mongo_client = pymongo.MongoClient()
    db = mongo_client.ducksdev

    total = db.monitoring.find().count()
    errfile = open('ERRORS', 'w')

    i = 0
    for doc in db.monitoring.find(timeout=False): # bad practice -> FIX!
        i += 1
        _id = str(doc['_id'])
        del doc['_id']
        
        # This should be customizable
        #if doc['servicename'] == 'checksumStatus':
            # doc['status'] := 0 | 1
            #doc['status'] = 'OK' if doc['status'] == 0 else 'NOK'

        try:
            es.index(index='ducksdev-%s' %doc['servicename'],
                     doc_type='monitoring',
                     id=_id,
                     body=doc)
        except elasticsearch.exceptions.RequestError as reqerr:
            errfile.write("CATCHED ERROR WITH:\n")
            errfile.write(str(doc)+'\n[')
            errfile.write(reqerr.error)
            errfile.write('] ')
            errfile.write(str(reqerr.info)+'\n')
            continue

        print '{0}\r'.format(str('{0:.2f}'.format(float(i)/total*100)) + '% ' + str(i) + '/' + str(total)),
        sys.stdout.flush()
    print
    errfile.close()"""
        

if __name__ == '__main__':
    main()
