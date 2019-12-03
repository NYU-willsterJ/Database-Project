import ZODB
from BTrees.IOBTree import BTree
import pandas as pd


class MyBTree:

    def __init__(self):
        self.T = BTree()
        self.data_list = []

    def insert(self, key, value):
        if key in self.T:  # if key already exists, replace with new value
            index = self.T.get(key)
            self.data_list[index][1] = value
        else:
            row = [key, value]
            self.data_list.append(row)
            self.T.insert(key, len(self.data_list) - 1)

    def search(self, key):
        index = self.T.get(key)
        if index is None:
            return 'Not present'
        val = self.data_list[index][1]
        if val is None:
            return 'Not present'
        return val

    def delete(self, key):
        if key in self.T:
            index = self.T.get(key)
            self.data_list[index][1] = None


