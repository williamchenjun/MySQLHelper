from dataclasses import dataclass
from typing import Union, TypedDict, Literal, Generic, TypeVar
import constants as constants
from datetime import datetime
import re

def string_wrapper(value):
    if isinstance(value,str):
        return f"'{value}'"
    return str(value)

def extract_data_type(column_definition):
    match = re.match(r'^\w+', column_definition)
    if match:
        return match.group(0)
    return None

def split_by_pattern(text, pattern):
    regex = re.compile(f'(?<!^)({pattern})')
    parts = regex.split(text)
    parts = [part.strip() for part in parts if part.strip()]
    return parts

class Components:
    def __init__(self) -> None:
        pass

@dataclass
class ColumnType(Components):
    name: str
    datatype: str
    autoincrement: bool = False
    constraint: Union[str, None] = None
    check: Union[str, None] = None
    default: Union[str, None] = None

    def get(self):
        assert extract_data_type(self.datatype.upper()) in constants.mysql_data_types.values(), "Unrecognised data type."
        if self.constraint is not None:
            assert self.constraint in constants.constraints.values(), "Unrecognised column type."
        
        text = f"{self.name} {self.datatype}"
        text += f" AUTO_INCREMENT" if self.autoincrement else ""
        text += f" {self.constraint}" if self.constraint is not None else ""
        text += f" CHECK({self.check})" if self.check is not None else ""
        text += f" DEFAULT {string_wrapper(self.default)}" if self.default is not None else ""

        return text 



@dataclass
class AggregateFunctionType(Components):
    """
    Represents an aggregation function.

    ```
    > from mysqlhelper.components import AggregateFunctionType
    > mysql = MySQL(...)
    > user_count = AggregateFunctionType("COUNT", "USER_ID").get() #returns: COUNT(USER_ID).
    > result = mysql.query.SELECT([user_count], table).get_results()
    ```
    """
    type: str
    column: Union[ColumnType, str]

    def get(self) -> str:
        return f"{self.type.upper()}({self.column.name if isinstance(self.column, ColumnType) else self.column})"

@dataclass
class ConstraintType(Components):
    """
    Builder for the MySQL `CONSTRAINT` keyword.
    """
    foreign_key: str
    references: str
    references_column: str
    constraint_name: Union[str, None] = None
    on_delete: Union[str, None] = None
    on_update: Union[str, None] = None
    unique: Union[tuple[str], list[str], None] = None
        
    def get(self):
        if self.on_delete is not None:
            assert self.on_delete.upper() in constants.on_update_delete.values(), f"ON DELETE can only be assigned the following values: {', '.join(constants.on_update_delete.values())}."
        if self.on_update is not None:
            assert self.on_update.upper() in constants.on_update_delete.values(), f"ON UPDATE can only be assigned the following values: {', '.join(constants.on_update_delete.values())}."
        

        text = f"CONSTRAINT {self.constraint_name}\n\t" if self.constraint_name is not None else ""
        text += f"FOREIGN KEY ({self.foreign_key}) \n"
        text += f"REFERENCES {self.references}({self.references_column})\n"
        text += f"ON DELETE {self.on_delete}\n" if self.on_delete is not None else ""
        text += f"ON UPDATE {self.on_update}\n" if self.on_update is not None else ""
        text += f"UNIQUE ({', '.join(self.unique)})" if self.unique is not None else ""

        return text
    
@dataclass
class Table(Components):
    """
    Represents a MySQL `TABLE`.
    """
    name: str
    columns: list[ColumnType, AggregateFunctionType, str]
    if_not_exists: bool = False
    constraints: Union[list[ConstraintType], None] = None

    def get(self) -> str:
        """
        Returns the `CREATE TABLE` query.
        """
        columns = [column.get() for column in self.columns]
        if self.constraints is not None:
            constraints = [constraint.get() for constraint in self.constraints]

        text = "CREATE TABLE "
        text += f"{'IF NOT EXISTS ' if self.if_not_exists else ''}{self.name}(\n"
        text += ",\n".join(columns)
        text += ",\n" if self.constraints is not None else "\n"
        text += ",\n".join(constraints) + ");" if self.constraints is not None else ");"
        
        return text
    

class WhenThen(TypedDict):
    when: str
    then: str

class SelectQuery:
    def __init__(self, query: str) -> None:
        self.query = query
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query
    
class FromQuery:
    def __init__(self, query: str) -> None:
        self.query = query
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query

class WhereQuery:
    def __init__(self, query: str) -> None:
        self.query = query
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query
    
class InsertQuery:
    def __init__(self, query: str) -> None:
        self.query = query
        self.columns: list[str] = []
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query
    
    def __columns__(self) -> list[str]:
        query = self.query.replace("INSERT INTO (", "").replace(")", "")
        self.columns = query.split(", ")
        return self.columns
    
class ValueQuery:
    def __init__(self, query: str) -> None:
        self.query = query
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query
    
class UpdateQuery:
    def __init__(self, query: str) -> None:
        self.query = query
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query

class DeleteQuery:
    def __init__(self, query: str) -> None:
        self.query = query
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query
    
class SetQuery:
    def __init__(self, query: str) -> None:
        self.query = query
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query

class ConstraintQuery:
    def __init__(self, query: str) -> None:
        self.query = query
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query

class GroupByQuery:
    def __init__(self, query: str) -> None:
        self.query = query
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query
    
class OrderByQuery:
    def __init__(self, query: str) -> None:
        self.query = query
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query
    
class HavingQuery:
    def __init__(self, query: str) -> None:
        self.query = query
    
    def __repr__(self) -> str:
        return self.query
    
    def __str__(self) -> str:
        return self.query

@dataclass
class Case(Components):
    """
    Builder class for `CASE...WHEN...THEN...ELSE`.
    """
    WhenThen: list[WhenThen]
    Var: Union[str, None] = None
    Else: Union[str, None] = None

    def get(self) -> str:
        """
        Returns the `CASE...WHEN...THEN...ELSE` query.
        """
        
        text = f"CASE {self.Var if self.Var is not None else ''}\n"
        text += (f"\tWHEN {when} THEN {then}\n" for whenthen in self.WhenThen for when, then in whenthen.items())
        text += "\t" if self.Else is not None else ""
        text += f"{'ELSE ' + self.Else if self.Else is not None else ''}END"

        return text

T = TypeVar("T", AggregateFunctionType, ColumnType, str)
U = TypeVar("U", bound = str)

class OperatorMethod(Generic[T, U]):
    def __init__(self, query: T, id: U = None) -> None:
        self.query = query
        self.id = id

    def __str__(self) -> str:
        return self.query

    def __repr__(self) -> str:
        return self.query
    
    def __methodname__(self) -> str:
        return self.id
    
    def get(self):
        return self.query

class Operators(Components):
    """
    Represents a MySQL operator.
    """
    def __init__(self) -> None:
        self.memory = dict(aliases = dict())
        super().__init__()
    
    def AND(self, left_condition: str, right_condition: str) -> OperatorMethod:
        """
        Both conditions must be true to return `True`.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.AND("COLUMN_1 > 1", "COLUMN_1 < 100")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """
        query = f"{left_condition} AND {right_condition}"
        return OperatorMethod(query, "AND")

    def OR(self, left_condition: str, right_condition: str) -> OperatorMethod:
        """
        At least one of the two conditions must be true to return `True`.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.OR("IN_STOCK=TRUE", "PREORDER_AVAIL=TRUE")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """
        query = f"{left_condition} OR {right_condition}"
        return OperatorMethod(query, "OR")

    def LIKE(self, column: Union[ColumnType, AggregateFunctionType, str], pattern: str) -> OperatorMethod:
        """
        Returns rows that satisfy the given pattern. Use `%` to represent zero, one or multiple characters; use `_` to represent a single character.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.LIKE("COL", "item-%")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """
        if isinstance(column, ColumnType):
            query = f"{column.name} LIKE {pattern}"
        elif isinstance(column, AggregateFunctionType):
            query = f"{column.get()} LIKE {pattern}"
        elif isinstance(column, str):
            query = f"{column} LIKE {pattern}"

        return OperatorMethod(query, "LIKE")

    def IN(self, column: Union[ColumnType, AggregateFunctionType, str], group: Union[str, list]) -> OperatorMethod:
        """
        Returns all rows whose values are included in the given set of values.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.IN("EXPIRATION_TIME", [1, 10, 15])
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """
        if isinstance(group, list):
            group = [string_wrapper(val) for val in group]
            group = ", ".join(group)

        if isinstance(column, ColumnType):
            query = f"{column.name} IN ({group})"
        elif isinstance(column, AggregateFunctionType):
            query = f"{column.get()} IN ({group})"
        elif isinstance(column, str):
            query = f"{column} IN ({group})"

        return OperatorMethod(query, "IN")

    def EQUAL(self, column: Union[ColumnType, AggregateFunctionType, str], value: Union[ColumnType, AggregateFunctionType, str, int, float]) -> OperatorMethod:
        """
        Returns all rows that are equal to the given value.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.EQUAL("FIRST_NAME", "Stephanie")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """

        LEFT = ""
        RIGHT = ""

        if isinstance(column, ColumnType):
            LEFT = column.name
        elif isinstance(column, AggregateFunctionType):
            LEFT = column.get()
        elif isinstance(column, str):
            LEFT = column

        if isinstance(value, ColumnType):
            RIGHT = value.name
        elif isinstance(value, AggregateFunctionType):
            RIGHT = value.get()
        elif isinstance(value, (str, int, float)):
            RIGHT = value

        query = f"{LEFT} = {RIGHT}"

        return OperatorMethod(query, "EQUAL")
        
    def GREATER(self, column: Union[ColumnType, AggregateFunctionType, str], value: Union[ColumnType, AggregateFunctionType, str, int, float]) -> OperatorMethod:
        """
        Returns all rows that are strictly greater than the given value.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.GREATER("AGE", "30")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """
        
        LEFT = ""
        RIGHT = ""

        if isinstance(column, ColumnType):
            LEFT = column.name
        elif isinstance(column, AggregateFunctionType):
            LEFT = column.get()
        elif isinstance(column, str):
            LEFT = column

        if isinstance(value, ColumnType):
            RIGHT = value.name
        elif isinstance(value, AggregateFunctionType):
            RIGHT = value.get()
        elif isinstance(value, (str, int, float)):
            RIGHT = value

        query = f"{LEFT} > {RIGHT}"

        return OperatorMethod(query, "GREATER")

    def LESS(self, column: Union[ColumnType, AggregateFunctionType, str], value: Union[ColumnType, AggregateFunctionType, str, int, float]) -> OperatorMethod:
        """
        Returns all rows that are strictly less than the given value.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.LESS("AGE", "30")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """

        LEFT = ""
        RIGHT = ""

        if isinstance(column, ColumnType):
            LEFT = column.name
        elif isinstance(column, AggregateFunctionType):
            LEFT = column.get()
        elif isinstance(column, str):
            LEFT = column

        if isinstance(value, ColumnType):
            RIGHT = value.name
        elif isinstance(value, AggregateFunctionType):
            RIGHT = value.get()
        elif isinstance(value, (str, int, float)):
            RIGHT = value

        query = f"{LEFT} < {RIGHT}"

        return OperatorMethod(query, "LESS")
    
    def EGREATER(self, column: Union[ColumnType, AggregateFunctionType, str], value: Union[ColumnType, AggregateFunctionType, str, int, float]) -> OperatorMethod:
        """
        Returns all rows that are greater than or equal to the given value.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.EGREATER("AGE", "30")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """

        LEFT = ""
        RIGHT = ""

        if isinstance(column, ColumnType):
            LEFT = column.name
        elif isinstance(column, AggregateFunctionType):
            LEFT = column.get()
        elif isinstance(column, str):
            LEFT = column

        if isinstance(value, ColumnType):
            RIGHT = value.name
        elif isinstance(value, AggregateFunctionType):
            RIGHT = value.get()
        elif isinstance(value, (str, int, float)):
            RIGHT = value

        query = f"{LEFT} >= {RIGHT}"

        return OperatorMethod(query, "EGREATER")

    def ELESS(self, column: Union[ColumnType, AggregateFunctionType, str], value: Union[ColumnType, AggregateFunctionType, str, int, float]) -> OperatorMethod:
        """
        Returns all rows that are less than or equal to the given value.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.ELESS("AGE", "30")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """

        LEFT = ""
        RIGHT = ""

        if isinstance(column, ColumnType):
            LEFT = column.name
        elif isinstance(column, AggregateFunctionType):
            LEFT = column.get()
        elif isinstance(column, str):
            LEFT = column

        if isinstance(value, ColumnType):
            RIGHT = value.name
        elif isinstance(value, AggregateFunctionType):
            RIGHT = value.get()
        elif isinstance(value, (str, int, float)):
            RIGHT = value

        query = f"{LEFT} <= {RIGHT}"

        return OperatorMethod(query, "ELESS")

    def AS(self, 
           entity: Union[ColumnType, AggregateFunctionType, Table, str, SelectQuery], 
           alias: str, 
           *, 
           _type: Literal[1, 2, 3] = 1) -> OperatorMethod: #either e.column, column AS e, or column e
        """
        Sets an alias for a column, table, subquery, or aggregator. Choose from the three types of alias types:
        1. `entity AS alias`
        2. `entity alias`
        3. `alias.entity`

        ```
        > mysql = MySQL(...)
        > age_col = mysql.query.AS("COLUMN_1", "AGE", _type = 1) # returns: COLUMN_1 AS AGE.
        > results = mysql.SELECT([age_col], table).get_results()
        ```
        """
        
        if _type == 1:
            suffix = f" AS {alias}"
            prefix = ""
        elif _type == 2:
            suffix = f" {alias}"
            prefix = ""
        elif _type == 3:
            suffix = ""
            prefix = f"{alias}."
            
        if isinstance(entity, (ColumnType, Table)):
            query = f"{prefix}{entity.name}{suffix}"
            self.memory["aliases"][entity.name] = alias
        elif isinstance(entity, AggregateFunctionType):
            query = f"{prefix}{entity.get()}{suffix}"
        elif isinstance(entity, str):
            query = f"{prefix}{entity}{suffix}"
            self.memory["aliases"][entity] = alias
        elif isinstance(entity, SelectQuery):
            assert _type != 3, "Select queries cannot be aliased by prefixes."
            query = f"({entity}){suffix}"

        return OperatorMethod(query, "AS")

    def BETWEEN_AND(self, column: Union[ColumnType, AggregateFunctionType, str], left_value: Union[int, str, datetime], right_value: Union[int, str, datetime], *, fmt: str = '%Y-%m-%d %H:%M:%S'):
        """
        Returns all rows that are between the provided two values (inclusive). The extrema can be either numbers or `datetime`.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.BETWEEN_AND("PRICE", 300, 400)
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """

        if isinstance(left_value, datetime):
            left_value = left_value.strftime(fmt)
        if isinstance(right_value, datetime):
            right_value = right_value.strftime(fmt)

        if isinstance(column, ColumnType):
            query = f"{column.name} BETWEEN {left_value} AND {right_value}"
        elif isinstance(column, AggregateFunctionType):
            query = f"{column.get()} BETWEEN {left_value} AND {right_value}"
        elif isinstance(column, str):
            query = f"{column} BETWEEN {left_value} AND {right_value}"

        return OperatorMethod(query, "BETWEEN_AND")

    def ISNULL(self, column: Union[ColumnType, AggregateFunctionType, str]):
        """
        Returns all the rows that are `NULL`.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.ISNULL("AGE")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """

        if isinstance(column, ColumnType):
            query = f"{column.name} IS NULL"
        elif isinstance(column, AggregateFunctionType):
            query = f"{column.get()} IS NULL"
        elif isinstance(column, str):
            query = f"{column} IS NULL"

        return OperatorMethod(query, "ISNULL")

    def ISNOTNULL(self, column: Union[ColumnType, AggregateFunctionType, str]):
        """
        Returns all the rows that are not `NULL`.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.ISNOTNULL("AGE")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """

        if isinstance(column, ColumnType):
            query = f"{column.name} IS NOT NULL"
        elif isinstance(column, AggregateFunctionType):
            query = f"{column.get()} IS NOT NULL"
        elif isinstance(column, str):
            query = f"{column} IS NOT NULL"

        return OperatorMethod(query, "ISNOTNULL")

    def STARTSWITH(self, column: Union[ColumnType, AggregateFunctionType, str], pattern: str):
        """
        Returns all the rows whose value (string) starts with the given pattern.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.STARTSWITH("FOOD_ITEM", "BACON")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """

        if isinstance(column, ColumnType):
            query = f"{column.name} LIKE '{pattern}%'"
        elif isinstance(column, AggregateFunctionType):
            query = f"{column.get()} LIKE '{pattern}%'"
        elif isinstance(column, str):
            query = f"{column} LIKE '{pattern}%'"

        return OperatorMethod(query, "STARTSWITH")


    def ENDSWITH(self, column: Union[ColumnType, AggregateFunctionType, str], pattern: str):
        """
        Returns all the rows whose value (string) ends with the given pattern.

        ```
        > mysql = MySQL(...)
        > condition = mysql.query.ENDSWITH("FOOD_ITEM", "SANDWICH")
        > results = mysql.SELECT(["*"], table, condition).get_results()
        ```
        """

        if isinstance(column, ColumnType):
            query = f"{column.name} LIKE '%{pattern}'"
        elif isinstance(column, AggregateFunctionType):
            query = f"{column.get()} LIKE '%{pattern}'"
        elif isinstance(column, str):
            query = f"{column} LIKE '%{pattern}'"

        return OperatorMethod(query, "ENDSWITH")

    def JOIN(self, _type: Literal["INNER", "OUTER", "LEFT", "RIGHT", "NATURAL", "CROSS"], left_table: Union[Table, str], right_table: Union[Table, SelectQuery, str], left_on: Union[ColumnType, str], right_on: Union[ColumnType, str]) -> OperatorMethod:
        """
        Joins two tables on a given column.

        ```
        > mysql = MySQL(...)
        > aliased_table_1 = mysql.query.AS(table_1, "T1", _type = 3)
        > aliased_table_2 = mysql.query.AS(table_2, "T2", _type = 3)
        > joined_tables = mysql.query.JOIN("INNER", aliased_table_1, aliased_table_2, left_on = "T1.id", right_on = "T2.id")
        > results = mysql.SELECT(["*"], joined_tables).get_results()
        ```
        """

        left_table_alias: str = self.memory["aliases"].get(left_table.name if isinstance(left_table, Table) else left_table)
        right_table_alias: str = self.memory["aliases"].get(right_table.name if isinstance(right_table, Table) else right_table)

        query = f"{left_table.name if isinstance(left_table, Table) else left_table} {_type} JOIN {right_table.name if isinstance(right_table, Table) else right_table} "
        query += f"ON {left_table_alias + '.' if left_table_alias is not None else ''}{left_on.name if isinstance(left_on, ColumnType) else left_on} = {right_table_alias + '.' if right_table_alias is not None else ''}{right_on.name if isinstance(right_on, ColumnType) else right_on}"

        return OperatorMethod(query, "JOIN")
        
class MySqlMethod:
    def __init__(self, query, name: str, data = None) -> None:
        self.query = query
        self.name = name
        self.data = data

    def __str__(self) -> str:
        return f"{self.data}"
    
    def __repr__(self) -> str:
        return f"{self.data}"

    def get(self) -> str:
        return self.query
    
    def __methodname__(self) -> str:
        return self.name

    def get_results(self):
        """
        Get the results of the `SELECT` query.
        """
        return self.data

class Query(Operators):
    """
    Represents a MySQL query.
    
    If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

    For example:

    ```
    self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
    
    ```
        
    """
    def __init__(self) -> None:
        self.components = list()
        super().__init__()

    def build(self) -> str:
        """
        Compose the entire MySQL query.
        """
        query = []
        for component in self.components:
            query.append(str(component))
        self.components = []
        return "\n".join(query) + ";"

    def Select(self,
               columns: list[Union[ColumnType, AggregateFunctionType, Case, str]] = "*",
               record: bool = True) -> SelectQuery:
        
        """
        Represents the singular `SELECT` statement. 
        
        If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

        For example:

        ```
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        
        ```
        
        """

        assert isinstance(columns, list), "The columns parameter should be a list, even if it contains a single item."

        query = "SELECT "
        columns_length = len(columns)

        if columns_length > 0:
            for i, column in enumerate(columns):
                if isinstance(column, ColumnType):
                    query += column.name
                elif isinstance(column, (AggregateFunctionType, Case)):
                    query += column.get()
                elif isinstance(column, str):
                    query += column

                if i < columns_length - 1: query += ", "
        else:
            query += columns
        
        if record: self.components.append(SelectQuery(query))

        return SelectQuery(query)

    def Insert(self, 
               table: Union[Table, str], 
               columns: list[Union[ColumnType, str]],
               record: bool = True) -> InsertQuery:
        """
        Represents the singular `INSERT INTO` statement.

        If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

        For example:

        ```
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        
        ```
        
        """

        assert all(not isinstance(column, OperatorMethod) for column in columns), "You cannot use aggregators or operators with INSERT."

        query = "INSERT INTO "

        if isinstance(table, Table):
            query += table.name
        elif isinstance(table, str):
            query += table
    
        query += "("

        for i, column in enumerate(columns):
            if isinstance(column, ColumnType):
                query += column.name
            elif isinstance(column, str):
                query += column
            
            if i < len(columns) - 1: query += ", "

        query += ")"

        if record: self.components.append(InsertQuery(query))

        return InsertQuery(query)

    def Update(self, 
               table: Union[Table, str],
               record: bool = True) -> UpdateQuery:
        """
        Represents the singular `UPDATE` statement.

        If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

        For example:

        ```
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        
        ```
        
        """

        query = "UPDATE "

        if isinstance(table, Table):
            query += table.name
        elif isinstance(table, str):
            query += table

        if record: self.components.append(UpdateQuery(query))

        return UpdateQuery(query)

    def Delete(self, 
               _from: Union[FromQuery, str], 
               where: Union[WhereQuery, str],
               record: bool = True) -> DeleteQuery:
        """
        Represents the `DELETE FROM...WHERE` statement.

        If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

        For example:

        ```
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        
        ```
        
        """

        query = "DELETE "
        if isinstance(_from, FromQuery):
            query += f"{_from.query} "
        elif isinstance(_from, str):
            query += f"FROM {_from} "

        if isinstance(where, WhereQuery):
            query += f"{where.query}"
        elif isinstance(where, str):
            query += f"WHERE {where}"

        if record: self.components.append(DeleteQuery(query))
        return DeleteQuery(query)
    
    def From(self, 
             table: Union[Table, str, SelectQuery],
               record: bool = True) -> FromQuery:
        """
        Represents the singular `FROM` keyword.

        If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

        For example:

        ```
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        
        ```
        
        """

        clause = "FROM "
        query = ""

        if isinstance(table, Table):
            query += table.name
        elif isinstance(table, str):
            query += table
        elif isinstance(table, SelectQuery):
            query += table.query
        
        query = clause + query
        
        if record: self.components.append(FromQuery(query))
        return FromQuery(query)
        
    def Set(self, 
            update: list[tuple[Union[ColumnType, str], Union[str, int, float, datetime]]],
            record: bool = True) -> SetQuery:
        """
        Represents the singular `SET` keyword.

        If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

        For example:

        ```
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        
        ```
        
        """

        query = "SET "

        for i, (column, value) in enumerate(update):
            if isinstance(column, ColumnType):
                query += f"{column.name}={string_wrapper(value)}"
            elif isinstance(column, str):
                query += f"{column}={string_wrapper(value)}"
            
            if i < len(update) - 1: query += ", "
            

        if record: self.components.append(SetQuery(query))
        return SetQuery(query)

    def Values(self, 
               values: Union[list, MySqlMethod],
               record: bool = True) -> ValueQuery:
        """
        Represents the singular `VALUES` keyword.

        If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

        For example:

        ```
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        
        ```
        
        """

        assert isinstance(self.components[-1], InsertQuery), "You need to precede this clause with an INSERT."
        assert len(self.components[-1].__columns__()) == len(values), "The number of values should match the number of columns."
        
        if isinstance(values, MySqlMethod) and values.__methodname__() == "select":
            query = values.get_results()
            if record: self.components.append(query)
            return query

        query = f"VALUES ({', '.join([string_wrapper(value) for value in values])})"
        if record: self.components.append(ValueQuery(query))
        return ValueQuery(query)

    def Where(self, 
              conditions: Union[str, list[str], OperatorMethod, list[OperatorMethod]],
              record: bool = True) -> WhereQuery:
        """
        Represents the singular `WHERE` keyword.

        If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

        For example:

        ```
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        
        ```
        
        """

        query = "WHERE "
        if isinstance(conditions, str):
            query += conditions
        elif isinstance(conditions, OperatorMethod):
            query += conditions.get()
        elif isinstance(condition, list):
            for i, condition in enumerate(conditions):
                if isinstance(condition, str):
                    query += condition
                elif isinstance(condition, OperatorMethod):
                    query += condition.get()

                if i < len(conditions) - 1: query += ",\n"

        if record: self.components.append(WhereQuery(query))
        return WhereQuery(query)
    
    def createTable(self, table: Table) -> str:
        return table.get()

    def dropTable(self, table: Union[Table, str]) -> str:
        return f"DROP TABLE {table.name if isinstance(table, Table) else table};"
    
    def groupBy(self, 
                columns: list[Union[ColumnType, str]],
               record: bool = True) -> GroupByQuery:
        query = "GROUP BY "

        for i, column in enumerate(columns):
            if isinstance(column, ColumnType):
                query += column.name
            elif isinstance(column, str):
                query += column

            if i < len(columns) - 1: query += ", "

        if record: self.components.append(GroupByQuery(query))
        return GroupByQuery(query)

    def Having(self, 
               conditions: Union[list[Union[OperatorMethod[Union[AggregateFunctionType, ColumnType, str], str], str]], str],
               record: bool = True) -> HavingQuery:
        """
        Represents the singular `HAVING` keyword.

        If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

        For example:

        ```
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        
        ```
        
        """

        query = "HAVING "

        if isinstance(conditions, str):
            query += conditions
        elif isinstance(conditions, list):
            for i, condition in enumerate(conditions):
                if isinstance(condition, str):
                    query += condition
                elif isinstance(condition, OperatorMethod):
                    query += str(condition)
                
                if i < len(conditions) - 1: query += ", "
        
        if record: self.components.append(HavingQuery(query))
        return HavingQuery(query)


    def orderBy(self, 
                columns: list[Union[ColumnType, AggregateFunctionType, str]], 
                ascending: bool = True,
                record: bool = True) -> OrderByQuery:
        """
        Represents the singular `ORDER BY` keyword.

        If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

        For example:

        ```
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        
        ```
        
        """

        query = "ORDER BY "

        for i, column in enumerate(columns):
            if isinstance(column, ColumnType):
                query += column.name
            elif isinstance(column, AggregateFunctionType):
                query += column.get()
            elif isinstance(column, str):
                query += column

            if not ascending: query += " DESC"

            if i < len(columns) - 1: query += ", "

        if record: self.components.append(OrderByQuery(query))
        return OrderByQuery(query)

    def Constraints(self, 
                    table: Table,
                    record: bool = True) -> ConstraintQuery:
        """
        Represents the singular `CONSTRAINTS` statement.

        If you want to nest `Query` methods, make sure to set `record` to `False` for the inner methods.

        For example:

        ```
        self.query.Delete(self.query.From(FROM, record = False), self.query.Where(WHERE, record = False))
        
        ```
        
        """

        query = ""
        for i, constraint in enumerate(table.constraints):
            query += constraint.get()
            if i < len(table.constraints) - 1: query += ",\n"

        if record: self.components.append(ConstraintQuery(query))
        return ConstraintQuery(query)