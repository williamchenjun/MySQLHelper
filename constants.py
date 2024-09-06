mysql_data_types = [
    "TINYINT",
    "SMALLINT",
    "MEDIUMINT",
    "INT",
    "INTEGER",
    "BIGINT",
    "DECIMAL",
    "DEC",
    "FLOAT",
    "DOUBLE",
    "DOUBLE PRECISION",
    "REAL",
    "BIT",
    "BOOLEAN",
    "BOOL",
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "TIME",
    "YEAR",
    "CHAR",
    "VARCHAR",
    "TINYTEXT",
    "TEXT",
    "MEDIUMTEXT",
    "LONGTEXT",
    "BINARY",
    "VARBINARY",
    "TINYBLOB",
    "BLOB",
    "MEDIUMBLOB",
    "LONGBLOB",
    "ENUM",
    "SET",
    "GEOMETRY",
    "POINT",
    "LINESTRING",
    "POLYGON",
    "MULTIPOINT",
    "MULTILINESTRING",
    "MULTIPOLYGON",
    "GEOMETRYCOLLECTION",
    "JSON"
]

mysql_data_types = {data_type.replace(" ","_"): data_type for data_type in mysql_data_types}

on_update_delete = {
    "CASCADE": "CASCADE", 
    "SET_NULL": "SET NULL", 
    "RESTRICT": "RESTRICT",
    "NO_ACTION": "NO ACTION",
    "SET_DEFAULT": "SET DEFAULT"
}

constraints = {
    "NOT_NULL": "NOT NULL",
    "NULL": "NULL",
    "PRIMARY_KEY": "PRIMARY KEY"
}