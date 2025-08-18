from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator

Param = dict[str, str | int | bool | dict]
ParamParent = dict[str, str | dict | int | bool | Param]
Params = dict[str, str | dict | int | bool | ParamParent]
ColumnTypes = None | str | float | int | bool
RowDict = dict[str, None | str | float | int | bool]
JSONList = list[RowDict]
BQQueryResult = None | RowIterator | _EmptyRowIterator
SchemaFieldFormat = dict[str, list[dict[str, str]]]
