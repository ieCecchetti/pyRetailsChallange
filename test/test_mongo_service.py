import unittest
from unittest import mock

from service.mongo_service import MongoService
import pandas as pd


class TestMongoService(unittest.TestCase):
    def test_mongo_connect(self):
        # test connection without db name -> raise RuntimeError
        self.assertIsNone(MongoService.mongo_connect(None, "test"))
        # test connection with Empty db name -> raise RuntimeError
        self.assertIsNone(MongoService.mongo_connect("", "test"))
        # test connection without collection name -> raise RuntimeError
        self.assertIsNone(MongoService.mongo_connect("db", None))
        # test connection with Empty collection name -> raise RuntimeError
        self.assertIsNone(MongoService.mongo_connect("db", ""))
        # test connection with wrong data(:port)
        self.assertIsNone(MongoService.mongo_connect("test", "online_retail", "localhost", 27018))
        # test connection with wrong data(:port)
        self.assertIsNotNone(MongoService.mongo_connect("test", "online_retail", "localhost", 27017))


    @mock.patch("pymongo.MongoClient")
    def test_mongo_import(self, mock_pymongo):
        data = []
        pd.DataFrame(data, columns=['A', 'B', 'C'])
        # test connection without db name -> raise RuntimeError
        self.assertFalse(MongoService.mongo_import(None, data))
        # test connection without collection name -> raise RuntimeError
        self.assertFalse(MongoService.mongo_import(mock_pymongo.initialize(), None))
