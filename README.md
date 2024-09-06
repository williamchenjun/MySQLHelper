# Python MySQL Helper Module (Pre-release)

This is a simple Python module that uses the `mysql-connector-python` module and simplifies the manipulation of MySQL databases and tables.

## How to Install
You can easily install the module using `pip`:

```bash
pip install git+https://github.com/williamchenjun/MySQLHelper.git@v0.1.0
```

or by going to the [releases page](https://github.com/williamchenjun/MySQLHelper/releases) and downloading the `mysqlhelper-v0.1.0.zip` file.

## Example Usage

```python
from mysqlhelper.backend import MySQL

from mysqlhelper.components import \
    Table, \
    ColumnType

mysql = MySQL(dict(
  user=...,
  password=...,
  host=...,
  port=...,
  database=...
))

# Create a table for users of website.
user_table = Table(
  name = "user_table",
  columns = [
    ColumnType("user_id", "INT", autoincrement = True, constraint = "PRIMARY KEY"),
    ColumnType("username", "VARCHAR(50)", constraint = "PRIMARY KEY"),
    # Add more as needed...
  ],
  if_not_exists = True
)

mysql.CREATE_TABLE(user_table)
...
mysql.INSERT(user_table, COLUMNS = user_table.columns, VALUES = [1, "username_1", ...])
...
users = mysql.SELECT(["*"], user_table, mysql.query.BETWEEN_AND("user_id", 1, 100)).get_results()

print(users)
# returns: [(1, "username_1", ...), (2, "username_2", ...), (3, "username_3", ...), ...]
```
