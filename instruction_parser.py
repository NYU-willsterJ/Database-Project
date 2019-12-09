from SQL import SQL
import re
from IOModule import IOModule

class InstructionParser:
    def __init__(self, data_folder, sql):
        self.sql = sql
        self.data_folder = data_folder
        self.IO = IOModule()

    def read_instruct(self, instruction):
        sql = self.sql

        s = str(instruction).replace(' ', '')
        if s.lower().find("btree") != -1 or s.lower().find("hash") != -1:  # create btree or hash
            s = re.split('\(', s)
            struct = s[0]
            delimiters = ',', '(', ')'
            regexPattern = '|'.join(map(re.escape, delimiters))
            tok = re.split(regexPattern, s[1])
            table_name = tok[0]
            column_name = tok[1]

            table = sql.table_dict[table_name]

            if struct.lower() == 'btree':
                sql.b_tree(table, column_name)
            elif struct.lower() == 'hash':
                sql.hash(table, column_name)

            return None

        elif s.lower().find("outputtofile") != -1:  # write to output
            length = len('outputtofile')
            r = s[length + 1: len(s)-1]
            table_name = r.split(',')[0]
            table = sql.table_dict[table_name]
            self.IO.write_output_table(table)

        elif s.lower().find("inputfromfile") != -1:  # load file
            ind = s.lower().find("inputfromfile")
            start_s = s[0:ind]
            ind2 = start_s.find(':=')
            table_name = start_s[0:ind2]
            s = s[ind:]
            tok2 = s.split('(')
            filename = self.data_folder + '/' + tok2[1][:len(tok2[1]) - 1] + '.txt'
            table = self.IO.load_data(filename, table_name)
            return table

        else:  # for normal instructions
            split_ind = s.find(':=')
            tok1 = s[split_ind+2:]
            output_table_name = s[0:split_ind]

            index_command = tok1.find('(')
            command = tok1[0: index_command]
            remainder = tok1[index_command:]
            remainder = remainder[1:len(remainder)-1]

            name_ind = remainder.find(',')
            table_name = remainder[0: name_ind]  # input table name. Should already exist
            table = sql.table_dict[table_name]

            if command == 'select':
                remainder = remainder[name_ind + 1:]
                [where_list_and, where_list_or] = self.__where_list(remainder)
                return self.sql.select(table, [], where_list_and, where_list_or, output_table_name)

            elif command == 'project':
                col_list = remainder.split(',')
                col_list.pop(0)
                return sql.select(table, col_list, [], [], output_table_name)

            elif command == 'avg':
                tok2 = remainder.split(',')
                return sql.avg(table, tok2[1], output_table_name)

            elif command == 'sum':
                tok2 = remainder.split(',')
                return sql.sum(table, tok2[1], output_table_name)

            elif command == 'count':
                tok2 = remainder.split(',')
                return sql.count(table, tok2[1], output_table_name)

            elif command == 'sumgroup':
                tok2 = remainder.split(',')
                tok2.pop(0)
                target_col = tok2.pop(0)
                return sql.sum_group(table, target_col, tok2, output_table_name)

            elif command == 'avggroup':
                tok2 = remainder.split(',')
                tok2.pop(0)
                target_col = tok2.pop(0)
                return sql.avg_group(table, target_col, tok2, output_table_name)

            elif command == 'countgroup':
                tok2 = remainder.split(',')
                tok2.pop(0)
                target_col = tok2.pop(0)
                return sql.count_group(table, target_col, tok2, output_table_name)

            elif command == 'join':
                tok2 = remainder.split(',')
                table1 = sql.table_dict[tok2.pop(0)]
                table2 = sql.table_dict[tok2.pop(0)]
                remainder = str(tok2)[2: len(tok2) - 3]
                [where_list_and, where_list_or] = self.__where_list(remainder)
                return sql.join(table1, table2, where_list_and, where_list_or, output_table_name)

            elif command == 'sort':
                tok2 = remainder.split(',')
                tok2.pop(0)
                return sql.sort(table, tok2, output_table_name)

            elif command == 'movavg':
                tok2 = remainder.split(',')
                tok2.pop(0)
                target_col = tok2.pop(0)
                window_size = int(tok2.pop(0))
                return sql.mov_avg(table, target_col, window_size, output_table_name)
            elif command == 'movsum':
                tok2 = remainder.split(',')
                tok2.pop(0)
                target_col = tok2.pop(0)
                window_size = int(tok2.pop(0))
                return sql.mov_sum(table, target_col, window_size, output_table_name)
            elif command == 'concat':
                tok2 = remainder.split(',')
                table1 = sql.table_dict[tok2[0]]
                table2 = sql.table_dict[tok2[1]]
                return sql.concat(table1, table2, output_table_name)

    def __where_list(self, remainder):
        sql = self.sql
        where_list_and = []
        where_list_or = []
        if remainder.find('and') != -1 or remainder.find('or') != -1:
            or_flag = False
            while (remainder.find('and') != -1 or remainder.find('or') != -1) is True:
                if remainder[0:2] == 'or':
                    or_flag = True
                start_ind = remainder.find('(')
                end_ind = remainder.find(')')
                where_cond_str = remainder[start_ind + 1:end_ind]
                remainder = remainder[end_ind + 1:]

                # parse where condition commands
                where_cond = self.__where_condition(where_cond_str)

                if or_flag is True:
                    where_list_or.append(where_cond)
                else:
                    where_list_and.append(where_cond)

        else:
            where_list_and = [self.__where_condition(remainder)]  # single ele list
        return where_list_and, where_list_or

    def __where_condition(self, s):
        left_s = ''
        right_s = ''
        op = ''
        for c in s:
            if (c.isalnum() or c == '.' or c == '_') and op == '':
                left_s += c
            elif not c.isalnum() and right_s == '':
                op += c
            else:
                right_s += c
        return left_s, op, right_s