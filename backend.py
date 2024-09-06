import mysql.connector
from typing import Union
from uuid import uuid4
from components import *

class Connector:
    """
    Represents the MySQL connector. This is the parent class of MySQL.
    """
    def __init__(self, credentials: dict, *, concatenate: bool = False) -> None:
        self.credentials = dict()
        self.concatenate = concatenate
        self.connector = None
        self.session_id = uuid4()
        self.__ALLOWED_KEYS__ = (
            "host",
            "user",
            "password",
            "database",
            "port"
        )

        self.results = []

        if all(key in self.__ALLOWED_KEYS__ for key in credentials.keys()):
            for key, val in credentials.items():
                self.credentials[key] = val
        
    def __get_credentials__(self) -> dict:
        return self.credentials

    def build(self):
        self.connector = mysql.connector.connect(**self.credentials)
        return self
    
class MySQL(Connector):
    """
    Represents a MySQL instance.
    """
    def __init__(self, credentials: dict, *, concatenate: bool = False) -> None:
        super().__init__(credentials, concatenate=concatenate)
        self.build()
        self.query = Query()

    def EXECUTE(self, query: Union[SelectQuery, InsertQuery, UpdateQuery, DeleteQuery, str], *args):
        """
        Execute a `MySQL` query. It can take a custom SQL query string, or a `SelectQuery`, `InsertQuery`, `UpdateQuery`, `DeleteQuery`.

        ```
        mysql = MySQL(...)
        mysql.EXECUTE("SELECT * FROM TABLE;")
        ```

        or you can access the MySQL query builder `MySQL.query`:

        ```
        mysql.query.Select(...)
        mysql.query.From(...)
        query = mysql.query.build()
        mysql.EXECUTE(query)
        ```
        """
        self.connector._open_connection()
        cursor = self.connector.cursor()
        cursor.execute(query)
        results = []
        if isinstance(query, SelectQuery) or query.upper().startswith("SELECT"):
            results = cursor.fetchall()
        cursor.close()
        self.connector.close()
        return results

    def CREATE_TABLE(self, 
                     TABLE: Union[str, Table], 
                     COLUMNS: list[ColumnType] = None, 
                     CONSTRAINTS: list[ConstraintType] = None, 
                     IF_NOT_EXISTS: bool = False) -> Table:
        """
        MySQL `CREATE TABLE` query method. Use this method to create a new table in your database.

        ```
        >> mysql = MySQL(...)
        >> mysql.CREATE_TABLE("TABLE", [ColumnType(...), ...], IF_NOT_EXISTS = True)
        ```

        or make use of the `Table` builder class from `mysqlhelper.components`:

        ```
        >> table = Table(
                name = "TABLE",
                columns = [
                    ColumnType(...),
                    ...
                ],
                constraints = ...,
                if_not_exists = ...
            )
        >> mysql.CREATE_TABLE(table)
        ```
        """
        
        if isinstance(TABLE, Table):
            table = TABLE
        else:
            table = Table(
                name = TABLE,
                columns = COLUMNS,
                constraints = CONSTRAINTS,
                if_not_exists = IF_NOT_EXISTS
            )

        query = self.query.createTable(table)

        self.EXECUTE(query)

        return table
    
    def SELECT(self,
               COLUMNS: list[Union[ColumnType, AggregateFunctionType, str]],
               FROM: Union[Table, MySqlMethod, str],
               WHERE: Union[str, list[str]] = None,
               JOIN: Union[list, tuple] = None,
               GROUP_BY: Union[str, ColumnType] = None,
               HAVING: Union[list[Union[OperatorMethod[Union[AggregateFunctionType, ColumnType, str], str], str]], str] = None,
               ORDER_BY: Union[ColumnType, AggregateFunctionType, str] = None
               ):
        """
        Represents the MySQL `SELECT` statement.
        
        Parameters
        ----------
        - `COLUMNS`: `list[ColumnType|AggregateFunctionType|str]` A list of columns.
        - `FROM`: `Table|MySqlMethod|str` Table you want to select from.
        - `WHERE`: `str|list[str]` (Optional) Condition(s) for the `SELECT` statement.
        - `JOIN`. `list|tuple` (Optional) Join two tables. Note: You must specify the following in the correct order `JOIN_TYPE`, `LEFT_TABLE` and `RIGHT_TABLE` e.g. `("INNER", table1, table2)`.
        - `GROUP_BY`: `str|ColumnType` (Optional) MySQL `GROUP BY` statement to aggregate rows by each unique value.
        - `HAVING`: `list[OperatorMethod|str]|str` (Optional) Can only be used if `GROUP_BY` is defined. MySQL `HAVING` statement used to filter aggregated columns.
        - `ORDER_BY`: `ColumnType|AggregateFunctionType|str` (Optional) Order table with respect to a given column or aggregator.

        Returns
        -------
        Returns `MySQLMethod`, which contains information about the executed query. The `get_results` method returns the query response. The `get` method returns the query.
        """
        
        self.query.Select(columns = COLUMNS)
        if JOIN:
            try:
                self.query.From(self.query.JOIN(*JOIN))
            except:
                raise ValueError("Please double check that your JOIN iterable is correct. Must be of length 5: [JOIN TYPE, LEFT TABLE, RIGHT TABLE, ON LEFT, ON RIGHT].")
        else:
            if isinstance(FROM, MySqlMethod) and FROM.__methodname__().lower() == "select":
                FROM = FROM.get()
            self.query.From(FROM)

        if WHERE: self.query.Where(WHERE)
        if GROUP_BY: self.query.groupBy(GROUP_BY)
        if HAVING: self.query.Having(HAVING)
        if ORDER_BY: self.query.orderBy(ORDER_BY)

        query = self.query.build()
        results = self.EXECUTE(query)

        return MySqlMethod(query, "select", results)
    
    def INSERT(self, INTO: Union[Table, str], 
               COLUMNS: list[Union[ColumnType, str]],
               VALUES: Union[list, MySqlMethod]
               ):
        """
        Represents the MySQL `INSERT` statement.

        Parameters
        ----------
        - `INTO`: `Table|str` The table to insert into.
        - `COLUMNS`: `list[ColumnType|str]` The columns that are being considered.
        - `VALUES`: `list|MySqlMethod` The values to be inserted into the table or a `SELECT` query. The latter will copy all the values from one table to the other.

        Returns
        -------
        Returns `MySQLMethod`, which contains information about the executed query. The `get` method returns the query.
        """

        self.query.Insert(INTO, COLUMNS)
        self.query.Values(VALUES)

        query = self.query.build()
        self.EXECUTE(query)

        return MySqlMethod(query, "insert")
    
    def UPDATE(self, 
               TABLE: Union[Table, str],
               SET: list[tuple[Union[ColumnType, str], Union[str, int, float, datetime]]],
               WHERE: Union[str, list[str]] = None
               ):
        """
        Represents the MySQL `UPDATE` statement.

        Parameters
        ----------
        - `TABLE`: `Table|str` The table to update.
        - `SET`: `list[ColumnType|str, str|int|float|datetime]` The update values.
        - `WHERE`: `str|list[str]` (Optional) Condition(s) for the `INSERT` statement.

        Returns
        -------
        Returns `MySQLMethod`, which contains information about the executed query. The `get` method returns the query.
        """

        self.query.Update(TABLE)
        self.query.Set(SET)
        if WHERE: self.query.Where(WHERE)

        query = self.query.build()
        
        self.EXECUTE(query)
        
        return MySqlMethod(query, "update")
    
    def DELETE(self,
               FROM: Union[Table, str, SelectQuery],
               WHERE: Union[str, list[str]]):
        """
        Represents the MySQL `DELETE` statement.

        Parameters
        ----------
        - `FROM`: `Table|str|SelectQuery` Table to delete from.
        - `WHERE`: `str|list[str]` (Options) Condition for the `DELETE` statement. Note: If it is not passed, all the rows will be deleted.

        Returns
        -------
        Returns `MySQLMethod`, which contains information about the executed query. The `get` method returns the query.
        """
        
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        query = self.query.build()
        self.EXECUTE(query)
        
        return MySqlMethod(query, "delete")
    
    def DROP_TABLE(self,
                   TABLE: Union[Table, str]) -> bool:
        """
        Represents the MySQL `DROP TABLE` statement.

        Parameters
        ----------
        - `TABLE`: `Table|str` Table to drop.

        Returns
        -------
        Returns `True` on success.
        """
        
        self.EXECUTE(self.query.dropTable(TABLE))

        return True