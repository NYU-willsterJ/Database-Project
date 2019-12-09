import os


class Table:

    def __init__(self, name):
        self.name = name
        self.column_dict = {}
        self.data_array = None
        self.group_count = {}

