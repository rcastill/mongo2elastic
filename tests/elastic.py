import elasticsearch

def main():
    es = elasticsearch.Elasticsearch(['http://elastic:changeme@localhost:9200'])
    es.index(index='pytest', doc_type='test', id=0, body={'name':'test0', 'meta':'None'})

if __name__ == '__main__':
    main()
