import re
import os

class IOModule:

    def load_data(self, file_path):
        data_list = []
        with open(file_path, 'r') as fp:
            print("inserting initial dataset...")
            line = fp.readline()  # read column names
            column_toks = line.split('|')
            
            line = fp.readline()
            while line:
                tok = line.split('|')
                tok[0] = int(tok[0])
                tok[1] = int(tok[1])
                data_list.append(tok)
                line = fp.readline()
            print("done.")
        return data_list

    def load_instructions(self, file_path):
        instr_list = []
        with open(file_path, 'r') as fp:
            line = fp.readline()
            while(line):
                tok = re.split('\(', line)
                tok[1] = tok[1].replace(' ', '')
                tokk = re.split(r'[(,)\n]', tok[1])
                instr = tok[0]
                key = int(tokk[0])
                if instr == 'insert':
                    val = int(tokk[1])
                    instr_list.append([instr, key, val])
                else:
                    instr_list.append([instr, key])
                line = fp.readline()
        return instr_list

    def write_output_table(self, data_list, time):
        if not os.path.isdir('./output'):
            os.mkdir('./output')

        with open('./output/results.txt', 'w') as fp:
            fp.write('total time = %f seconds\n\n' % time)

            for item in data_list:
                fp.write('%d | %d\n' % (item[0], item[1]))
