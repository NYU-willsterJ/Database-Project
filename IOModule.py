import re
import os
import numpy as np
from Table import Table
from SQL import SQL

class IOModule:

    def load_data(self, file_path, name):
        with open(file_path, 'r') as fp:
            print("inserting data into " + name + " table...")
            line = fp.readline()  # read column names
            column_toks = line.strip().split('|')
            column_dict = {val : index for index, val in enumerate(column_toks)}
            column_size = len(column_toks)

            data_array = np.zeros((0, column_size), dtype=object)

            line = fp.readline()
            while line:
                if str(line) == '\n':
                    fp.readline()
                    continue
                tok = line.strip().split('|')

                if not len(tok) == column_size:
                    print('ERROR - input data cols not match with col headers size')
                    exit(1)

                vector = np.zeros((1,column_size), dtype=object)
                for i, val in enumerate(tok):
                    vector[0][i] = val

                data_array = np.append(data_array, vector, axis=0)

                line = fp.readline()

            table = Table(name)
            table.column_dict = column_dict
            table.data_array = data_array

        return table

    def load_instructions(self, file_path):
        instr_list = []
        with open(file_path, 'r') as fp:
            line = fp.readline()
            while line:
                if(line.startswith('//')) or (str(line) == '\n') or (str(line) == ''):
                    line = fp.readline()
                    continue
                line = str(line)
                index_truncate = line.strip().find('//')
                line = line[0:index_truncate]
                instr_list.append(line)

                line = fp.readline()
        return instr_list

    def write_output_table(self, table):
        output_folder = './output'
        if not os.path.isdir(output_folder):
            os.mkdir(output_folder)

        output_path = output_folder + '/' + table.name + '.txt'

        with open(output_path, 'w') as fp:
            col_count = 0
            for key in table.column_dict:
                fp.write(key)
                if col_count < len(table.column_dict) - 1:
                    fp.write('|')
                else:
                    fp.write('\n')
                col_count += 1
            for item in table.data_array:
                nparray_str = str(item)
                nparray_str = nparray_str.replace('\n', '')
                nparray_str = nparray_str[1:len(nparray_str)-1]
                nparray_str = nparray_str.replace('\'', '')
                nparray_str = nparray_str.replace(' ', '|')
                fp.write(nparray_str)
                fp.write('\n')


