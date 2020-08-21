import pymongo as db

server = "mongodb://localhost:27017"
data_base = "stock"
class db_connector:
    def link_start(tableName):
        my_client = db.MongoClient(server)
        db_list = my_client.list_database_names()
        if data_base in db_list:
            print("Get Connection...")
            my_db = my_client[data_base]
            col_list = my_db.list_collection_names()
            if tableName in col_list:
                print("Get Collection...")
                return my_db[tableName]
        return None


    def row_insert(collection, row):
        x = collection.insert_one(row)

    def rows_insert(collection, rows):
        x = collection.insert_many(rows)
        print(x.inserted_ids)

    def rows_select(collection, params=None, filter=None):
        print(params)
        return collection.find(params, filter)

