"""
Copyright 2020, Institute for Systems Biology

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

old_rel = 'r25'
new_rel = 'r26'

# comparing two releases, which fields only appear in one
field_diff = """
    SELECT table_name AS release, field_path AS field
    FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    WHERE field_path IN (
        SELECT field_path 
        FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        WHERE table_name='{}_clinical' 
            OR table_name='{}_clinical'
       GROUP BY field_path
       HAVING COUNT(field_path) <= 1)
""".format(old_rel, new_rel)


# comparing two releases for contradictory data types
data_type_diff = """
    SELECT field_path, data_type, COUNT(field_path) AS distinct_data_type_cnt 
    FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    WHERE (table_name='{}_clinical' OR table_name='{}_clinical')
        AND (data_type = 'INT64' OR data_type = 'FLOAT64' OR data_type = 'STRING' OR data_type = 'BOOL')
    GROUP BY field_path, data_type 
    HAVING distinct_data_type_cnt <= 1
""".format(old_rel, new_rel)


# if param 1 is new release and param 2 is old release, find added case_ids; else, find removed
diff_case_ids = """
    SELECT * 
    FROM `isb-project-zero`.GDC_Clinical_Data.{}_clinical
    WHERE case_id NOT IN (
        SELECT case_id 
        FROM `isb-project-zero`.GDC_Clinical_Data.{}_clinical)
""".format(new_rel, old_rel)


# diff table counts
programs_with_different_number_of_tables_query = """
    WITH old_table_cnts AS (
      SELECT program, COUNT(program) AS num_tables 
      FROM (
        SELECT els[OFFSET(1)] AS program
        FROM (
          SELECT SPLIT(table_name, '_') AS els
          FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.TABLES
          WHERE table_name LIKE '{}%'))
      WHERE program != 'clinical'
      GROUP BY program
    ),
    new_table_cnts AS (
      SELECT program, COUNT(program) AS num_tables 
      FROM (
        SELECT els[OFFSET(1)] AS program
        FROM (
          SELECT SPLIT(table_name, '_') AS els
          FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.TABLES
          WHERE table_name LIKE '{}%'))
      WHERE program != 'clinical'
      GROUP BY program
    )
    
    SELECT  o.program AS prev_rel_program_name, 
            n.program AS new_rel_program_name, 
            o.num_tables AS prev_table_cnt, 
            n.num_tables AS new_table_cnt
    FROM new_table_cnts n
    FULL OUTER JOIN old_table_cnts o
      ON o.program = n.program
    WHERE o.num_tables != n.num_tables
      OR o.num_tables IS NULL or n.num_tables IS NULL
    ORDER BY n.num_tables DESC
""".format(old_rel, new_rel)

# get tables list
program_tables_list_query = """
    SELECT (
        SELECT els[OFFSET(1)] AS program
    ) AS program,
    (
        SELECT TRIM(STRING_AGG(table_fg, ' '), '0 ')
        FROM UNNEST(els) AS table_fg 
        WITH OFFSET index
        WHERE index BETWEEN 2 AND 100
    ) AS table_name
    FROM (
        SELECT SPLIT(table_name, '_') AS els
        FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.TABLES
        WHERE table_name LIKE '{}%'
        AND table_name != '{}_clinical')
    ORDER BY program
""".format(new_rel)



#### END CASES DATA VALIDATION



# not for validation -- shows where naming conflicts could occur
repeated_fields = """
    SELECT field, count(field) AS occur 
    FROM (  SELECT ARRAY_REVERSE(SPLIT(field_path, '.'))[OFFSET(0)] as field
            FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
            WHERE (table_name='{}_clinical') 
                AND (data_type = 'INT64' 
                OR data_type = 'FLOAT64' 
                OR data_type = 'STRING' 
                OR data_type = 'BOOL')
       GROUP BY field_path
       ORDER BY field
       )
    GROUP BY field
    HAVING count(field) > 1
    ORDER BY occur DESC
""".format(new_rel, new_rel)


# create biospecimen stub table
biospecimen_stub = """
    SELECT project_name, case_gdc_id, case_barcode, sample_gdc_id, sample_barcode
    FROM (
        SELECT proj, case_gdc_id, case_barcode, 
            SPLIT(sample_ids, ', ') AS s_gdc_ids, 
            SPLIT(submitter_sample_ids, ', ') AS s_barcodes
        FROM (
            SELECT case_id AS case_gdc_id, 
                submitter_id AS case_barcode, sample_ids, submitter_sample_ids, 
            SPLIT(
            (SELECT project_id
                FROM UNNEST(project)), '-')[OFFSET(0)] AS project_name
            FROM `isb-project-zero.GDC_Clinical_Data.{}_clinical`
            )
        ), 
        UNNEST(s_gdc_ids) as sample_gdc_id WITH OFFSET pos1, 
        UNNEST(s_barcodes) as sample_barcode WITH OFFSET pos2
        WHERE pos1 = pos2
        ORDER BY proj, case_gdc_id
""".format(new_rel)


