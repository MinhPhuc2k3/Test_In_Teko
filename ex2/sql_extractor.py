import re
inputs = dict()
outputs = dict()

print('opening file sql_file.sql')
f = open('./sql_file.sql', 'r')


file_content = f.read().lower()
pattern = '[a-z0-9_]+\\.[a-z0-9_]+\\.[a-z0-9_]+'
statements = ['create table', 'insert into', 'update']
drop_table_statement = 'drop table'
last_index = 0
inset  = 1

print('processing...')
file_content = re.sub("\\s+", " ", file_content)
list_tables = re.findall(pattern, file_content)
for table in list_tables:
    last_index = file_content.find(table, last_index)
    before_str = file_content[last_index-15:last_index-1]
    is_ouput = False
    for output_statement in statements:
        if before_str.find(output_statement) != -1:
            outputs[table] = inset
            is_ouput = True
            break
    if is_ouput == False:
        inputs[table] = inset
    if before_str.find(drop_table_statement) != -1:
        inputs[table] = 1- inset
        outputs[table] = 1 - inset
f.close()
print('sql_file.sql has been closed')

print('opening io.yaml')
yaml_file = open('./io.yaml', 'w+')
yaml_file.writelines('inputs:\n')
for line in inputs.keys():
    if inputs[line] == 1:
        yaml_file.writelines(' - '+line+'\n')
yaml_file.writelines('outputs:\n')
for line in outputs.keys():
    if outputs[line] == 1:
        yaml_file.writelines(' - '+line+'\n')
yaml_file.close()
print('io.yaml has been closed')

print('Sucessfull!')