import os
from typing import Optional, Self

import psycopg2
import rich.console
import rich.panel
from rich import inspect

# Useful reference material on PostgreSQL metadata:
#
# https://www.postgresql.org/docs/current/functions-info.html
# https://www.postgresql.org/docs/current/runtime-config-preset.html - Show xxxx options - server
# https://www.postgresql.org/docs/current/runtime-config-client.html - Show xxxx options - client

# PostgreSQL Python interface documentation:
#
# https://www.psycopg.org/docs/index.html

# Rich documentation
#
# https://rich.readthedocs.io/en/stable/index.html


# Console object for controlling Rich output
console = rich.console.Console()


def get_connection_string() -> Optional[str]:
    try:
        pwd = os.environ["PGPW"]
    except Exception as err:
        print(f"** No PGPW environment variable set")
        return None

    # connection_string = f"host=127.0.0.1 port=5432 user=postgres password={pwd}"
    connection_string = f"user=postgres password={pwd}"
    return connection_string


class BasicDBInfo:
    def __init__(self):
        self.session_user = ""  # Used to create the session
        self.current_user = ""  # Active role - same as session_user unless something like Set Role has updated it for the session
        self.current_database = ""  # current_database and current_catalog are the same thing 'database' is pg, 'catalog' is SQL standard.
        self.pg_version = ""


# NB Seem to need to use the '.extensions' module to do Typing of the connection object:
# https://www.psycopg.org/docs/extensions.html
# https://www.psycopg.org/docs/extensions.html
def read_basics(conn: psycopg2.extensions.connection) -> tuple[BasicDBInfo, str]:
    cur = conn.cursor()
    cur.execute("SELECT session_user, current_user, current_database(), version();")
    f = cur.fetchone()
    basics = BasicDBInfo()
    basics.session_user = f[0]
    basics.current_user = f[1]
    basics.current_database = f[2]
    basics.pg_version = f[3]

    basics_str = (
        f""
        + f"Session user: {basics.session_user}\n"
        + f"Current user: {basics.current_user}\n"
        + f"Current database (=catalog): {basics.current_database}\n"
        + f"PG Version: {basics.pg_version}"
    )

    return basics, basics_str


def main() -> None:
    connection_string: Optional[str] = get_connection_string()

    if connection_string == None:
        return

    # https://www.psycopg.org/docs/module.html#psycopg2.connect
    # https://www.postgresql.org/docs/current/libpq-envars.html
    # https://www.postgresql.org/docs/current/libpq-pgpass.html
    try:
        conn = psycopg2.connect(connection_string)
    except Exception as err:
        console.print(f"[red]** failed to connect using[/red] {connection_string}")
        return

    # If turned on, the second time round the loop does a 'set role' to the built-in read-only role, and shows how
    # this modifies the 'Current user' value reported.
    switch_to_read_role = False

    for i in [1, 2] if switch_to_read_role else [1]:
        if i == 2:
            conn.cursor().execute("Set Role pg_read_all_data ")

        # Type-checking syntax restriction. Would like to do something like this:
        #   (basics, basics_str) : tuple[BasicDBInfo, str] = read_basics(conn)
        # but it's not allowed by Python typing spec.
        # If we do just:
        #   basics, basics_str = read_basics(conn)
        # then the variables end up with type 'Any'
        inspect(conn)
        result: tuple[BasicDBInfo, str] = read_basics(conn)
        basics: BasicDBInfo = result[0]
        basics_str: str = result[1]

        panel = rich.panel.Panel(basics_str, title="Session info")
        console.print(panel)

    # Show info about the connection

    conn_info = conn.info
    conn_info_str = (
        f""
        + f"DSN : {conn.dsn}\n"
        + f"DSN parameters: {conn_info.dsn_parameters}\n"
        + f"DBname : {conn_info.dbname}\n"
        + f"User : {conn_info.user}\n"
        + f"Password : {'*' * len(conn_info.password)}\n"
        + f"Host : {conn_info.host}\n"
        + f"Port : {conn_info.port}\n"
        + f"Options : {conn_info.options}\n"
        + f"Status : {conn_info.status}\n"
        + f"Protocol version: {conn_info.protocol_version}\n"
        + f"Server version: {conn_info.server_version}\n"
        + f"Used password : {conn_info.used_password}\n"
        + f"SSL in use : {conn_info.ssl_in_use}\n"
        + f"Autocommit: {conn.autocommit}\n"
        + f"Isolation level: {conn.isolation_level}\n"
        + f"Read-only: {conn.readonly}\n"
        + f"Async: {conn.async_}\n"
        + f"Client encoding: {conn.encoding}"
    )

    panel = rich.panel.Panel(conn_info_str, title="Connection info")
    console.print(panel)


if __name__ == "__main__":
    main()
