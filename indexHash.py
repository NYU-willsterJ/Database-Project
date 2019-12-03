
class Hash:

    def __init__(self):
        self.T = {}
        self.data_list = []

    def insert(self, key, value):
        if key in self.T:  # if key already exists, replace with new value
            index = self.T[key]
            self.data_list[index][1] = value
        else:
            row = [key, value]
            self.data_list.append(row)
            self.T[key] = len(self.data_list) - 1

    def search(self, key):
        index = self.T[key]
        if index is None:
            return 'Not present'
        val = self.data_list[index][1]
        if val is None:
            return 'Not present'
        return val

    def delete(self, key):
        if key in self.T:
            index = self.T[key]
            self.data_list[index][1] = None
