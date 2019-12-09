
class Hash:

    def __init__(self):
        self.T = {}

    def insert(self, key, value):
        if key in self.T:  # if key already exists, replace with new value
            index_list = self.T.get(key)
            index_list.append(value)  # append to list of indices of data array
            self.T[key] = index_list  # update
        else:
            index_list = [value]
            self.T[key] = index_list

    def search(self, key):
        if key not in self.T:
            return None

        return self.T[key]
