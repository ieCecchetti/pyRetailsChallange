from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, InvalidName
import logging


class MongoService:
    """ Mongo Service class containing all methods for using mongo and python """

    def __init__(self):
        raise RuntimeError("Constructor cannot be called.")

    @staticmethod
    def mongo_connect(db_name, coll_name, db_url='127.0.0.1', db_port=27017):
        """
        Imports a csv file at path csv_name to a mongo collection
        returns: the collection selected
        """
        logger = logging.getLogger("my logger")
        if db_name is None:
            logger.error("[Error] DB name is null.")
            return None
        if coll_name is None:
            logger.error("[Error] Collection name is null")
            return None
        try:
            m_client = MongoClient(db_url, db_port)
            m_db = m_client[db_name]
            m_collection = m_db[coll_name]
            # check if the connection is actually established
            # The ismaster command is cheap and does not require auth.
            m_client.admin.command('ismaster')
            logger.info("Connection established at: [%s:%d]" % (db_url, db_port))
            return m_collection
        except ConnectionFailure:
            logger.error("[Error] Server not available cannot establish connection")
            return None
        except InvalidName as e:
            logger.error("[Error] Server not available cannot establish connection. Caused by %s" % e)
            return None

    @staticmethod
    def mongo_import(m_collection: Collection, data) -> bool:
        """
        Imports the in in input into the Mongo DB
        """
        logger = logging.getLogger("my logger")
        try:
            if m_collection is None:
                logger.error("[Error] Collection is null. It not exists or DB Failed to Connect.")
                return False
            if data is None:
                logger.error("[Error] Data input is null. File not loaded or Empty")
                return False
            res = m_collection.insert_many(data.to_dict('records'))
            logger.info("Inserted #%d records." % len(res.inserted_ids))
        except InvalidName as e:
            logger.error("[Error] Server not available cannot establish connection. Caused by %s" % e)
            return False