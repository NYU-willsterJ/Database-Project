from IOModule import IOModule
from indexBTree import MyBTree
from indexHash import Hash
from SQL import SQL
from instruction_parser import InstructionParser
import argparse

parser = argparse.ArgumentParser(description='parse input args')
parser.add_argument('--data_folder', default='./data', help='specify data folder where .txt files are located')
parser.add_argument('--instruction_path', default='./data/instructions.txt', help='instruction.txt file')

args = parser.parse_args()
data_folder = args.data_folder
instruction_path = args.instruction_path

IO = IOModule()
sql = SQL()

instruction_list = IO.load_instructions(instruction_path)
parser = InstructionParser(data_folder, sql)

for instruct in instruction_list:
    print('exec: ' + instruct)
    table = parser.read_instruct(instruct)
    if table is not None:
        sql.table_dict[table.name] = table
print("done.")

'''
R = IO.load_data('./data/sales1.txt', name='R')
R1 = sql.select(R, [], [['time', '>', '50']], [['qty', '<', '30']], name='R1')
R2 = sql.select(R1, ['saleid', 'qty', 'pricerange'], [], [], 'R2')
R3 = sql.avg(R1, 'qty', 'R3')
R4 = sql.sum_group(R1, 'time', ['qty'], 'R4')
R5 = sql.sum_group(R1, 'qty', ['time', 'pricerange'], 'R5')
R6 = sql.avg_group(R1, 'qty', ['pricerange'], 'R6')

S = IO.load_data('./data/sales2_test.txt', 'S')
T = sql.join(R, S, [['R.customerid', '=', 'S.C']], [], 'T')

T1 = sql.join(R1, S, [['R1.saleid', '=', 'S.saleid']], [], 'T1')
T2 = sql.sort(T1, ['S_C'], 'T2')
T2prime = sql.sort(T1, ['R1_time', 'S_C'], 'T2prime')
T3 = sql.mov_avg(T2prime, 'R1_qty', 3, 'T3')
T4 = sql.mov_sum(T2prime, 'R1_qty', 5, 'T4')


Q1 = sql.select(R, [], [['qty', '=', '5']], [], 'Q1')

sql.b_tree(R, 'qty')

Q2 = sql.select(R, [], [['qty', '=', '5']], [], 'Q2')
Q3 = sql.select(R, [], [['itemid', '=', '7']], [], 'Q3')
sql.hash(R, 'itemid')
Q4 = sql.select(R, [], [['itemid', '=', '7']], [], 'Q2')
Q5 = sql.concat(Q4, Q2, 'Q5')
'''