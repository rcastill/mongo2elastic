import elasticsearch
import pymongo
import json
import sys

def main():
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
        if doc['servicename'] == 'checksumStatus':
            # doc['status'] := 0 | 1
            doc['status'] = 'OK' if doc['status'] == 0 else 'NOK'

        try:
            es.index(index='ducksdev2',
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
    errfile.close()
        

if __name__ == '__main__':
    main()
