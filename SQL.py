from Table import Table
import numpy as np
import copy
from indexBTree import MyBTree
from indexHash import Hash


class SQL:
    def __init__(self):
        self.Btree = MyBTree()
        self.Hash = Hash()
        self.called_btree = False
        self.called_hash = False
        self.table_dict = {}

    def select(self, table, select_col_list, where_list_and, where_list_or, name):
        """
        Implements both row and column selection
        @params: - select_col_list : [sales, qty, ...]
                 - where_list : [column, operator, operand]
        """
        data_array = table.data_array

        if (self.called_btree or self.called_hash) is True:
            data_array =  self.__hashed_data_array(table, where_list_and)
            if len(data_array) == 0:  # if returns empty
                data_array = table.data_array

        output_table = Table(name)
        if len(select_col_list) == 0:  # choose ALL columns
            if len(where_list_and) == 0 and len(where_list_or) == 0:  # choose ALL rows
                output_table = table  # return the entire table
            else:  # return table that meet conditions
                output_table.column_dict = table.column_dict
                output_table.data_array = np.zeros((0, len(table.column_dict)), dtype=object)
                for item in data_array:
                    to_add_or = False  # set OR condition
                    for [column, operator, operand] in where_list_or:
                        col_index = table.column_dict[column]
                        condition = self.__operator_operand_bool(operator, operand, item, col_index)
                        if condition is True:
                            to_add_or = True

                    if len(where_list_and) == 0:
                        to_add_and = False  # set AND condition
                    else:
                        to_add_and = True

                    for [column, operator, operand] in where_list_and:
                        col_index = table.column_dict[column]
                        condition = self.__operator_operand_bool(operator, operand, item, col_index)
                        to_add_and = to_add_and and condition  # boolean or

                    if (to_add_and or to_add_or) is True:
                        item = np.expand_dims(item, axis=0)
                        output_table.data_array = np.append(output_table.data_array, item, axis=0)
        else:  # if select columns (i.e. PROJECTION on cols)
            output_table.data_array = np.zeros((0, len(select_col_list)), dtype=object)
            output_table.column_dict = {val: i for i, val in enumerate(select_col_list)}

            if len(where_list_and) == 0 and len(where_list_or) == 0:  # no condition, import all rows
                for item in data_array:
                    vector = np.zeros((1, len(select_col_list)), dtype=object)
                    for col_name, col_index in output_table.column_dict.items():
                        input_table_col_index = table.column_dict[col_name]
                        val = item[input_table_col_index]
                        vector[0][col_index] = val
                    output_table.data_array = np.append(output_table.data_array, vector, axis=0)
            else:  # conditions on rows
                for item in data_array:
                    to_add_or = False  # set OR condition
                    for [column, operator, operand] in where_list_or:
                        col_index = table.column_dict[column]
                        condition = self.__operator_operand_bool(operator, operand, item, col_index)
                        if condition is True:
                            to_add_or = True

                    if len(where_list_and) == 0:
                        to_add_and = False  # set AND condition
                    else:
                        to_add_and = True

                    for [column, operator, operand] in where_list_and:
                        col_index = table.column_dict[column]
                        condition = self.__operator_operand_bool(operator, operand, item, col_index)
                        to_add_and = to_add_and and condition

                    if (to_add_and or to_add_or) is True:  # if row is to be added, select column for output table
                        vector = np.zeros((1, len(select_col_list)), dtype=object)
                        for col_name, col_index in output_table.column_dict.items():
                            input_table_col_index = table.column_dict[col_name]
                            val = item[input_table_col_index]
                            vector[0][col_index] = val
                        output_table.data_array = np.append(output_table.data_array, vector, axis=0)
        return output_table

    def avg(self, table, target_col, name):
        output_table = Table(name)
        output_table.column_dict = {'avg(' + target_col + ')': 0}

        sum = 0
        for item in table.data_array:
            col_index = table.column_dict[target_col]
            sum += int(item[col_index])

        avg = sum / len(table.data_array)
        output_table.data_array = np.zeros((1, 1), dtype=object)
        output_table.data_array[0][0] = avg
        return output_table

    def sum(self, table, target_col, name):
        sum = 0
        for item in table.data_array:
            col_index = table.column_dict[target_col]
            sum += int(item[col_index])

        output_table = Table(name)
        output_table.column_dict['sum(' + target_col + ')'] = 0
        output_table.data_array = np.array([[sum]], dtype=object)
        return output_table

    def count(self, table, target_col, name):
        count = len(table.data_array)

        output_table = Table(name)
        output_table.column_dict['count(' + target_col + ')'] = 0
        output_table.data_array = np.array([[count]], dtype=object)
        return output_table

    def group_by(self, table, target_col, by_list):
        group_dict = {}
        group_index = 0

        lst = [target_col]
        lst.extend(by_list)

        table = self.select(table, lst, [], [], 'temp')  # reduce columns by target and by_list

        for i in range(len(table.data_array)):
            vector = table.data_array[i]
            val_group_list = []
            for bys in by_list:
                col_index = table.column_dict[bys]
                val = vector[col_index]
                val_group_list.append(val)  # group identifier
            identifier = str(val_group_list)

            if identifier not in group_dict:  # if new group identified
                temp_table = Table(group_index)
                group_dict[identifier] = temp_table
                group_index += 1
                temp_table.column_dict = copy.deepcopy(table.column_dict)
                temp_table.data_array = copy.deepcopy(np.expand_dims(vector, axis=0))
            else:  # if group identifier exists, append vector to group table data array
                temp_table = group_dict[identifier]
                temp_table.data_array = np.append(temp_table.data_array, np.expand_dims(vector, axis=0), axis=0)

        return group_dict

    def sum_group(self, table, target_col, by_list, name):
        """
        @params: - target_col : column_name to sum
                 - by_list : ['pricerange', 'time']
        """
        output_table = Table(name)
        dict_list = [str("count(" + target_col + ")")]
        dict_list.extend(by_list)
        output_table.column_dict = {col_name: ind for ind, col_name in enumerate(dict_list)}
        output_table.data_array = np.zeros((0, len(by_list) + 1), dtype=object)
        group_dict = self.group_by(table, target_col, by_list)

        for identifier, group_table in group_dict.items():
            sum_table = self.sum(group_table, target_col, 'count')
            data_array = np.expand_dims(group_table.data_array[0], axis=0)  # get the first row of data array
            data_array[0][0] = sum_table.data_array[0][0]
            output_table.data_array = np.append(output_table.data_array, data_array, axis=0)

        return output_table

    def avg_group(self, table, target_col, by_list, name):
        output_table = Table(name)
        dict_list = [str("count(" + target_col + ")")]
        dict_list.extend(by_list)
        output_table.column_dict = {col_name: ind for ind, col_name in enumerate(dict_list)}
        output_table.data_array = np.zeros((0, len(by_list) + 1), dtype=object)
        group_dict = self.group_by(table, target_col, by_list)

        for identifier, group_table in group_dict.items():
            avg_table = self.avg(group_table, target_col, 'avg')
            data_array = np.expand_dims(group_table.data_array[0], axis=0)  # get the first row of data array
            data_array[0][0] = avg_table.data_array[0][0]
            output_table.data_array = np.append(output_table.data_array, data_array, axis=0)

        return output_table

    def count_group(self, table, target_col, by_list, name):
        output_table = Table(name)
        dict_list = [str("count(" + target_col + ")")]
        dict_list.extend(by_list)
        output_table.column_dict = {col_name: ind for ind, col_name in enumerate(dict_list)}
        output_table.data_array = np.zeros((0, len(by_list) + 1), dtype=object)

        group_dict = self.group_by(table, target_col, by_list)  # get dict of groups

        for identifier, group_table in group_dict.items():
            count_table = self.count(group_table, target_col, 'count')
            data_array = np.expand_dims(group_table.data_array[0], axis=0)  # get the first row of data array
            # data_array = np.column_stack((count_table.data_array[0], data_array))
            data_array[0][0] = count_table.data_array[0][0]
            output_table.data_array = np.append(output_table.data_array, data_array, axis=0)

        return output_table

    def join(self, table1, table2, where_list_and, where_list_or, name):
        output_table = Table(name)
        output_table.data_array = np.zeros((0, len(table1.data_array[0]) + len(table2.data_array[0])), dtype=object)

        table1 = copy.deepcopy(table1)
        table2 = copy.deepcopy(table2)

        if len(where_list_and) == 0 and len(where_list_or) == 0:
            pass
        else:
            # rename the dictionary keys by adding prefix of table name
            temp1 = {}
            temp2 = {}
            temp3 = {}
            for key in table1.column_dict:
                temp1[table1.name + '.' + key] = table1.column_dict[key]
                temp3[table1.name + '_' + key] = table1.column_dict[key]
            for key in table2.column_dict:
                temp2[table2.name + '.' + key] = table2.column_dict[key]
                temp3[table2.name + '_' + key] = table2.column_dict[key] + len(temp1)
            table1.column_dict = temp1
            table2.column_dict = temp2

            output_table.column_dict = temp3

            for i in range(len(table1.data_array)):
                for j in range(len(table2.data_array)):
                    to_add_or = False  # set OR condition
                    for [column1, operator, column2] in where_list_or:
                        condition = self.__join_condition(column1, operator, column2, table1, table2, i, j)
                        if condition is True:
                            to_add_or = True

                    if len(where_list_and) == 0:
                        to_add_and = False  # set AND condition
                    else:
                        to_add_and = True

                    for [column1, operator, column2] in where_list_and:
                        condition = self.__join_condition(column1, operator, column2, table1, table2, i, j)
                        to_add_and = condition and to_add_and

                    if (to_add_and or to_add_or) is True:
                        vector = np.zeros((1, 0), dtype=object)
                        vector = np.append(vector, np.expand_dims(table1.data_array[i], axis=0), axis=1)
                        vector = np.append(vector, np.expand_dims(table2.data_array[j], axis=0), axis=1)
                        output_table.data_array = np.append(output_table.data_array, vector, axis=0)
        return output_table

    def sort(self, table, sort_list, name):
        if len(sort_list) == 0:
            return table

        sort_col = sort_list.pop(0)  # sort by first criteria
        col_index = table.column_dict[sort_col]
        output_array = table.data_array[table.data_array[:, col_index].astype(int).argsort()]

        # divide table into tables by the already sorted elements
        grouped_table_value = {}
        grouped_table_index = 0
        table_group = []
        for item in output_array:
            value = item[col_index]  # get the sorted value of each data
            vector = copy.deepcopy(np.expand_dims(item, axis=0))
            if value not in grouped_table_value:  # if new group of sorted values encountered...
                grouped_table_value[value] = grouped_table_index
                temp_table = Table('value')  # create new table
                temp_table.column_dict = table.column_dict
                table_group.append(temp_table)
                temp_table.data_array = vector  # assign first data entry
                grouped_table_index += 1
            else:
                temp_table.data_array = np.append(temp_table.data_array, vector, axis=0)  # append to the already existing table

        output_array = np.zeros((0, len(table.data_array[0])), dtype=object)  # reset table
        for table_i in table_group:
            sorted_table = self.sort(table_i, sort_list, table_i.name)
            output_array = np.append(output_array, sorted_table.data_array, axis=0)

        output_table = Table(name)
        output_table.column_dict = table.column_dict
        output_table.data_array = copy.deepcopy(output_array)

        return output_table

    def b_tree(self, table, target_col):
        self.called_btree = True
        self.called_hash = False
        for i in range(len(table.data_array)):
            vector = table.data_array[i]
            col_index = table.column_dict[target_col]
            col_val = vector[col_index]
            self.Btree.insert(col_val, i)

    def hash(self, table, target_col):
        self.called_btree = False
        self.called_hash = True
        for i in range(len(table.data_array)):
            vector = table.data_array[i]
            col_index = table.column_dict[target_col]
            col_val = vector[col_index]
            self.Hash.insert(col_val, i)

    def mov_avg(self, table, target_col, window_size, name):
        output_table = Table(name)
        output_table.column_dict = table.column_dict
        output_table.data_array = np.zeros((0, len(table.column_dict)), dtype=object)

        for i in range(len(table.data_array)):
            col_index = table.column_dict[target_col]
            vector = copy.deepcopy(table.data_array[i])
            sum = 0
            curr_index = i
            count = 0
            while (count < window_size) and (curr_index >= 0):
                shadow_vector = table.data_array[curr_index]
                sum += int(shadow_vector[col_index])
                count += 1
                curr_index -= 1
            vector[col_index] = sum / count
            output_table.data_array = np.append(output_table.data_array, np.expand_dims(vector, axis=0), axis=0)

        return output_table

    def mov_sum(self, table, target_col, window_size, name):
        output_table = Table(name)
        output_table.column_dict = table.column_dict
        output_table.data_array = np.zeros((0, len(table.column_dict)), dtype=object)

        for i in range(len(table.data_array)):
            col_index = table.column_dict[target_col]
            vector = copy.deepcopy(table.data_array[i])
            sum = 0
            curr_index = i
            count = 0
            while (count < window_size) and (curr_index >= 0):
                shadow_vector = table.data_array[curr_index]
                sum += int(shadow_vector[col_index])
                count += 1
                curr_index -= 1
            vector[col_index] = sum
            output_table.data_array = np.append(output_table.data_array, np.expand_dims(vector, axis=0), axis=0)

        return output_table

    def concat(self, table1, table2, name):
        output_table = Table(name)
        output_table.column_dict = table1.column_dict
        output_table.data_array = np.zeros((0, len(table1.data_array[0])), dtype=object)

        output_table.data_array = np.append(output_table.data_array, table1.data_array, axis=0)
        output_table.data_array = np.append(output_table.data_array, table2.data_array, axis=0)

        return output_table

    def __operator_operand_bool(self, operator, operand, table_entry, col_index):
        if operator == '=':
            if table_entry[col_index] == operand:
                return True
            else:
                return False
        elif operator == '<':
            if table_entry[col_index] < operand:
                return True
            else:
                return False
        elif operator == '>':
            if table_entry[col_index] > operand:
                return True
            else:
                return False
        elif operator == '!=':
            if not table_entry[col_index] == operand:
                return True
            else:
                return False
        elif operator == '>=':
            if table_entry[col_index] >= operand:
                return True
            else:
                return False
        elif operator == '<=':
            if table_entry[col_index] <= operand:
                return True
            else:
                return False

    def __join_condition(self, column1, operator, column2, table1, table2, row_ind1, row_ind2):
        col_ind1 = table1.column_dict[column1]
        col_ind2 = table2.column_dict[column2]
        val1 = table1.data_array[row_ind1][col_ind1]
        val2 = table2.data_array[row_ind2][col_ind2]

        if operator == '=':
            if val1 == val2:
                return True
            else:
                return False
        elif operator == '<':
            if val1 < val2:
                return True
            else:
                return False
        elif operator == '>':
            if val1 > val2:
                return True
            else:
                return False
        elif operator == '!=':
            if val1 != val2:
                return True
            else:
                return False
        elif operator == '<=':
            if val1 <= val2:
                return True
            else:
                return False
        elif operator == '>=':
            if val1 >= val2:
                return True
            else:
                return False

    def __hashed_data_array(self, table, where_list_and):

        if self.called_hash is True:
            struct = self.Hash
        else:
            struct = self.Btree

        output_array = np.zeros((0, len(table.data_array[0])), dtype=object)

        for [column, operator, operand] in where_list_and:
            index_list = struct.search(operand)
            for index in index_list:
                vector = table.data_array[index]
                output_array = np.append(output_array, np.expand_dims(vector, axis=0), axis=0)

        return output_array