"""
Copyright 2020-2021, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

if 'find_like_columns' in steps:
    file_type_dicts = {}

    with open(file_traversal_list, mode='r') as traversal_list_file:
        all_files = traversal_list_file.read().splitlines()

    for file_path in all_files:
        file_name = file_path.split('/')[-1]
        file_name_no_ext = ".".join(file_name.split('.')[:-1])

        if program == "TCGA":
            file_type = "_".join(file_name_no_ext.split('_')[2:-1])
        elif program == "TARGET":
            file_type = file_name_no_ext.split("_")[-1]

        if file_type not in file_type_dicts:
            file_type_dicts[file_type] = list()

        file_type_dicts[file_type].append(file_path)

    for file_type, file_list in file_type_dicts.items():
        header_dict = dict()

        print(f"\n***{program}: {file_type}***\n")

        for idx, file_path in enumerate(file_list):
            file_name = file_path.split('/')[-1]
            file_name_no_ext = ".".join(file_name.split('.')[:-1])

            print(f"{idx}\t{file_name_no_ext}")

        for idx, file_path in enumerate(file_list):
            with open(file_path, 'r') as fh:
                headers = fh.readline().strip().split('\t')
                for header in headers:
                    if header not in header_dict:
                        header_dict[header] = list()
                    header_dict[header].append(idx)

        sorted_headers = " ".join(sorted(header_dict, key=lambda key: len(header_dict[key]), reverse=True))
        header_list = sorted_headers.split(" ")

        for col_name in header_list:
            print(f"{col_name}\t{header_dict[col_name]}")

if 'find_duplicates_in_tables' in steps:
    duplicate_key_tables = []
    no_duplicate_key_tables = []
    with open(tables_file, 'r') as tables_fh:
        id_key = programs[program]['id_key']

        table_ids = tables_fh.readlines()
        for table_id in table_ids:
            query = f"""
                SELECT {id_key}, COUNT({id_key})
                FROM {table_id}
                GROUP BY {id_key}
                HAVING COUNT({id_key}) > 1
            """

            results = bq_harness_with_result(sql=query,
                                             do_batch=False,
                                             verbose=False)
            total_rows = results.total_rows
            if total_rows > 0:
                duplicate_key_tables.append(table_id)
            else:
                no_duplicate_key_tables.append(table_id)

    print(f"Tables with duplicate id keys:")
    for duplicate_key_table in duplicate_key_tables:
        print(duplicate_key_table)

    print(f"Tables with no duplicate id keys:")
    for no_duplicate_key_table in no_duplicate_key_tables:
        print(no_duplicate_key_table)

if 'find_matching_target_usis' in steps:
    if program == 'TARGET':
        id_key_map = dict()
        id_key = programs[program]['id_key']
        with open(tables_file, 'r') as tables_fh:
            table_ids = tables_fh.readlines()

        for idx, table_id in enumerate(table_ids):
            print(f"{idx}: {table_id.strip()}")

        for idx, table_id in enumerate(table_ids):
            query = f'''
            SELECT {id_key}
            FROM {table_id.strip()}
            '''

            results = bq_harness_with_result(sql=query,
                                             do_batch=False,
                                             verbose=False)

            for row in results:
                patient_barcode = row[0]
                if patient_barcode not in id_key_map:
                    id_key_map[patient_barcode] = list()
                id_key_map[patient_barcode].append(idx)

        for patient_barcode in sorted(id_key_map):
            if len(id_key_map[patient_barcode]) > 1:
                print(f"{patient_barcode}: {id_key_map[patient_barcode]}")

