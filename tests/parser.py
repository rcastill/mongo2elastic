class InvalidSyntax(Exception):
    pass

class CollectionDoesNotExist(Exception):
    pass

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
                raise InvalidSyntax("DB cannot be empty")
            db = mongo_client[dbname]

            # Details should at least contain instruction
            details = split[1]
            if len(details) == 0:
                raise InvalidSyntax("Details cannot be empty")

            # Instruction must be + or -
            instr = details[0]
            if instr not in ('+', '-'):
                raise InvalidSyntax("Instruction must be +|-")

            # Get collection names in details
            collections = details[1:].strip().split()
            # Get collections in database
            collection_names = db.collection_names()

            # Check if selected collections exist
            if instr == '+':
                for collection in collections:
                    if collection not in collection_names:
                        raise CollectionDoesNotExist(collection)
                return collections
            # Blacklist detailed collections
            else:
                return list(
                    set(collection_names) -
                    set(collections))
