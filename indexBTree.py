from BTrees.OOBTree import BTree


class MyBTree:

    def __init__(self):
        self.T = BTree()

    # key: indexing_key, value: index to array
    def insert(self, key, value):
        if key in self.T:  # if key already exists, append
            index_list = self.T.get(key)
            index_list.append(value)  # append to list of indices of data array
            self.T[key] = index_list  # update
        else:
            index_list = [value]
            self.T.insert(key, index_list)

    def search(self, key):
        index_list = self.T.get(key)
        if index_list is None:
            return None

        return index_list




