# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
def clear_statement(statement: str) -> str:
    """
    Clear unnecessary spaces and semicolons at the statement end.

    Oracle-specific: adds semicolon after END statement.

    Examples
    --------

    >>> clear_statement("SELECT * FROM mytable")
    'SELECT * FROM mytable'
    >>> clear_statement("SELECT * FROM mytable ; ")
    'SELECT * FROM mytable'
    >>> clear_statement("CREATE TABLE mytable (id NUMBER)")
    'CREATE TABLE mytable (id NUMBER)'
    >>> clear_statement("BEGIN ... END")
    'BEGIN ... END;'
    """

    statement = statement.rstrip().lstrip("\n\r").rstrip(";").rstrip()
    if statement.lower().endswith("end"):
        statement += ";"
    return statement
