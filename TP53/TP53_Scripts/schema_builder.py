import json 
import concurrent.futures
import os 
import sys 

def create_schema(file_name):
    
    
    file_name_and_ext = os.path.basename(file_name)
    basename = os.path.splitext(file_name_and_ext)[0]
    
    
    strings = ['nvarchar',
               'char',
               'nchar',
               'varchar',
               'ntext']
    
    integers = ['mediumint',
                'smallint',
                'tinyint',
                'int']
    
    floats = ['decimal',
              'float',
              'real']
    
    json_schema = []
    with open(file_name, 'r') as sql_file: 
        for line in sql_file:
            json_dict = {}
            if 'NULL' in line or 'NOT NULL' in line:
                if 'SET' not in line:
                    line_split = line.strip().split(']')
                    
                    name = line_split[0]
                    
                    if '[' in name:
                        name = name.strip('[')
                        if ']' in name: 
                            name = name.strip(']')
                            if '(%)' in name:
                                name = name.strip('%')
                    
                    
                    if len(name.split()) > 1: 
                        name = f'{name[0]}_{name[1]}'
                
                    data_type = line_split[1].split('[')[1]                    
                    
                    if data_type in integers:
                        data_type = 'integer'
                        json_dict['name'] = name
                        json_dict['type'] = data_type
                        json_schema.append(json_dict)
                        
                    if data_type in strings:
                        data_type = 'string'
                        json_dict['name'] = name
                        json_dict['type'] = data_type
                        json_schema.append(json_dict)
                        
                    if data_type in floats:
                        data_type = 'float'
                        json_dict['name'] = name
                        json_dict['type'] = data_type
                        json_schema.append(json_dict)

                    if data_type == 'bit':
                        data_type = 'boolean'
                        json_dict['name'] = name
                        json_dict['type'] = data_type
                        json_schema.append(json_dict)
                    

    with open(os.path.abspath(f'../P53_Database/P53_data_schema/{basename}.json'), 'w') as out_file:
        
        json.dump(json_schema,
                   out_file,
                   indent=4
                   )


def main():
    
    arg = input('Would you like to run this in parallel? (Y/n): ')
    
    file_names =  ['../P53_Database/P53/dbo.AA_change.Table.sql',
                   '../P53_Database/P53/dbo.AA_codes.Table.sql',
                #    '../P53_Database/P53/dbo.GermlineRefView.Table.sql',
                #    '../P53_Database/P53/dbo.GermlineView.Table.sql',
                #    '../P53_Database/P53/dbo.SomaticView.Table.sql'
                ]


    abs_path = [os.path.abspath(a_file) for a_file in file_names]
    

    # Synchronous 
    if arg.lower() == 'n' or arg.lower() == 'no':
        for a_file in abs_path:
            create_schema(a_file)
    
    
    # Parallelized 
    if arg.lower() == 'y' or arg.lower() == 'yes':
        with concurrent.futures.ProcessPoolExecutor() as executor:
            executor.map(create_schema, abs_path)


    

if __name__ == '__main__':
    main()