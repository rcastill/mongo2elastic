import pymongo

def main():
    client = pymongo.MongoClient()
    db = client.ducksdev
    one = db.monitoring.find_one()
    print one

if __name__ == '__main__':
    main()
