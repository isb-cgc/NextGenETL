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

# todo: based on setting in yaml config, either:
#  - call api and get _mapping for field groups specified in field group name list string,
#    then use that response to dynamically generate a json schema file.
#  - pass pre-build schema file to use in creating the bq table

"""
NOTE: schema file syntax:

[
    {"name": "field_name", "description": "Documentation info", type="integer"}, 
    {"name": "field_name_2", "description": "Documentation info", type="string"} 
]
"""

"""
PLAN:

retrieve schema file, if exists
else, generate json file from mappings:
 - get field_groups from yaml config
 - get associated fields from _mapping endpoint, u

"""