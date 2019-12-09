import logging
import threading
import time
import numpy as np

dt = np.dtype([('name', np.unicode_, 16), ('grades', np.float64)])

test = np.zeros((2,5), dtype=str)

for i in range(len(test)):
    for j in range(len(test[0])):
        test[i][j] = 2

print(test)

'''
sum group

        col_size = len(by_list) + 1

        output_table = Table(name)
        output_table.column_dict[target_col] = 0
        for i, val in enumerate(by_list):
            output_table.column_dict[val] = i + 1
        output_table.data_array = np.zeros((0, col_size), dtype=object)

        group_dict = {}  # {by_cols : index of output array}
        output_array_index = 0

        col_list = []
        col_list.append(target_col)
        col_list.extend(by_list)
        temp = self.select(table, col_list, [], [], 'temp')
        for item in temp.data_array:
            col_val_list = []
            for bys in by_list:
                col_val = item[temp.column_dict[bys]]
                col_val_list.append(col_val)
            key = str(col_val_list)
            if key not in group_dict:  # if new group detected, create output row
                group_dict[key] = output_array_index
                output_table.group_count[key] = 1  # population per group. to be used for avg_group
                output_array_index += 1

                vector = np.zeros((1, col_size), dtype=object)
                col_index = temp.column_dict[target_col]
                val = item[col_index]
                vector[0][0] = val

                for i in range(0, len(col_val_list)):  # create replacement vector
                    vector[0][i+1] = col_val_list[i]
                output_table.data_array = np.append(output_table.data_array, vector, axis=0)  # update output vector
            else:  # update value of list-by group
                output_table.group_count[key] += 1  # increment group population
                col_index = temp.column_dict[target_col]
                val = item[col_index]
                group_index = group_dict[key]
                prev_val = int(output_table.data_array[group_index][0])
                val = str(int(val) + int(prev_val))

                vector = np.zeros((1, len(by_list) + 1), dtype=object)  # create replacement vector
                vector[0][0] = val
                for i in range(0, len(by_list)):
                    vector[0][i+1] = output_table.data_array[group_index][i+1]

                output_table.data_array[group_index] = vector  # update to new vector

'''