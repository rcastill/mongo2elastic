def filter(doc):
    for data in doc['data']:
        doc['%s_free' %data['name']] = data['free']
        doc['%s_total' %data['name']] = data['total']
