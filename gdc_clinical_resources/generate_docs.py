"""
Copyright 2020, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import json

"""

'''
SELECT distinct(diag__primary_diagnosis), count(diag__primary_diagnosis) as diag_count
FROM `isb - project - zero.GDC_Clinical_Data.rel23_clin_FM` 
GROUP BY diag__primary_diagnosis
ORDER BY diag_count DESC
LIMIT 10
'''

documentation:
- list of programs with record counts, most common primary diagnoses?


- list of tables, with total record counts, schemas, id keys, reference data


- list of columns with types, descriptions and possible counts (frequency distribution?)


- data source citation
"""


def main():
    with open('files/rel23_documentation.json', 'r') as json_file:
        doc_json = json.load(json_file)

        print json.dumps(doc_json, indent=2, sort_keys=True)



if __name__ == '__main__':
    main()
