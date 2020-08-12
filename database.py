from pymongo import MongoClient, ReturnDocument, errors
from bson.objectid import ObjectId
import pprint, sys

class DBConnection:
    def __init__(
        self,
        host: str,
        port: int,
    ):
        self.host = host
        self.port = port
        try:
            self.connection = MongoClient(host, port)
            self.connection.server_info()
        except errors.ServerSelectionTimeoutError as err:
            print(err)
            sys.exit(0)

    def set_database(self, database: str):
        self.db = self.connection[f'{database}']

    def set_collection(self, collection: str):
        self.collection = self.db[f'{collection}']

    def get_records(
        self,
        limit: int = 0,
        to_skip: int = 0,
    ):
        for record in self.collection.find().skip(to_skip).limit(limit):
            yield record

    def check_listings(self):
        error_records = self.collection.find({"offer.listing.does_not_exist": True}).count()
        processed_records = self.collection.find({"offer.listing.is_processed": True}).count()
        all_records = self.collection.find().count()
        print(all_records - processed_records - error_records)

        if all_records - processed_records - error_records == 0:
            self.collection.update_many(
                {"offer.listing.is_processed": True},
                {"$set": {"offer.listing.is_processed": False}}
            )

    def execute_bulk_write(self, query: list):
        self.collection.bulk_write(query)
