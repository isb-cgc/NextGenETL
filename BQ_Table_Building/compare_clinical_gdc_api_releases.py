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

# comparing two releases, which field_paths only appear in one
field_diff = """
    SELECT field_path, table_name FROM
     `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    WHERE field_path in
      (SELECT field_path FROM
       `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
       where (table_name='r24_clinical' or table_name='r25_clinical')
       GROUP BY field_path
       HAVING count(field_path) <= 1
       )
"""

# comparing two releases for contradictory data types
data_type_diff = """
    SELECT field_path, data_type, count(field_path) as cnt FROM
    `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    where (table_name='r24_clinical' or table_name='r25_clinical') 
    AND (data_type = 'INT64' OR data_type = 'FLOAT64' OR data_type = 'STRING' OR data_type = 'BOOL')
    GROUP BY field_path, data_type 
    HAVING cnt <= 1
"""

new_case_ids = """
    SELECT * FROM
       `isb-project-zero`.GDC_Clinical_Data.r25_clinical
       where case_id not in (
        SELECT case_id FROM
        `isb-project-zero`.GDC_Clinical_Data.r24_clinical
       )
"""

removed_case_ids = """
SELECT * FROM
   `isb-project-zero`.GDC_Clinical_Data.r24_clinical
   where case_id not in (
    SELECT case_id FROM
    `isb-project-zero`.GDC_Clinical_Data.r25_clinical
   )
"""

repeated_fields = """
SELECT field, count(field) as occur from
( SELECT ARRAY_REVERSE(SPLIT(field_path, '.'))[OFFSET(0)] as field
  FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
   WHERE (table_name='r25_clinical') 
   AND (data_type = 'INT64' OR data_type = 'FLOAT64' OR data_type = 'STRING' OR data_type = 'BOOL')
   GROUP BY field_path
   ORDER BY field
   )
   GROUP BY field
   HAVING count(field) > 1
   ORDER BY occur DESC
"""