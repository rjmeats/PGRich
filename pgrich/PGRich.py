# Allow forward types in annotations. Python may never implement this as the default.
from __future__ import annotations

import os
from typing import Optional, Self, TypeAlias

import psycopg2
import rich.console
import rich.panel
import rich.tree
from rich import inspect

#

# Useful reference material on PostgreSQL metadata:
#
# https://www.postgresql.org/docs/current/functions-info.html
# https://www.postgresql.org/docs/current/runtime-config-preset.html - Show xxxx options - server
# https://www.postgresql.org/docs/current/runtime-config-client.html - Show xxxx options - client
# https://www.postgresql.org/docs/15/catalogs.html - Postgres system catalogues
# https://www.postgresql.org/docs/15/views.html - Postgres system views built on the system catalgues

# PostgreSQL Python interface documentation:
#
# https://www.psycopg.org/docs/index.html

# Rich documentation
#
# https://rich.readthedocs.io/en/stable/index.html


# Type aliases for easier reference to Postgres types
# NB Seem to need to use the '.extensions' module to do Typing of the connection object:
# https://www.psycopg.org/docs/extensions.html
# https://www.psycopg.org/docs/extensions.html
pg_connection_type: TypeAlias = psycopg2.extensions.connection


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


class BasicSessionInfo:
    def __init__(self):
        self.session_user = ""  # Used to create the session
        self.current_user = ""  # Active role - same as session_user unless something like Set Role has updated it for the session
        self.current_database = ""  # current_database and current_catalog are the same thing 'database' is pg, 'catalog' is SQL standard.
        self.pg_version = ""
        self.search_path = ""  # Order of searching schema names (in the current database) to find an unqualified object in a query
        self.effective_search_path_list = []
        self.cluster_name = ""


def read_session_basics(conn: pg_connection_type) -> tuple[BasicSessionInfo, str]:
    # https://www.postgresql.org/docs/15/functions-info.html
    cur = conn.cursor()
    cur.execute("select session_user, current_user, current_database(), version();")
    f = cur.fetchone()
    basics = BasicSessionInfo()
    basics.session_user = f[0]
    basics.current_user = f[1]
    basics.current_database = f[2]
    basics.pg_version = f[3]

    # Read the search path setting, and its practical implementation result
    # https://www.postgresql.org/docs/15/view-pg-settings.html
    # https://www.postgresql.org/docs/15/functions-info.html
    # Could/should perhaps use the current_setting function instead ?
    # https://www.postgresql.org/docs/15/functions-admin.html
    cur = conn.cursor()
    cur.execute(
        "select setting, current_schemas(True) from pg_settings where name = 'search_path';"
    )
    f = cur.fetchone()
    basics.search_path = f[0]
    basics.effective_search_path_list = f[1]

    # Come up with something to provide a cluster name.
    cur = conn.cursor()
    cur.execute(
        "select setting, inet_server_addr(), inet_server_port() from pg_settings where name = 'cluster_name';"
    )
    f = cur.fetchone()
    basics.cluster_name = f[0]
    server_ip = f[1]
    server_port = f[2]
    if basics.cluster_name == "":
        basics.cluster_name = f"<Unnamed cluster> @{server_ip}/{server_port}"

    basics_str = (
        f""
        + f"Session user: {basics.session_user}\n"
        + f"Current user: {basics.current_user}\n"
        + f"Current database (=catalog): {basics.current_database}\n"
        + f"Search path: {basics.search_path}\n"
        + f"PG Version: {basics.pg_version}\n"
        + f"Cluster name: {basics.cluster_name}\n"
    )

    return basics, basics_str.strip()


def summarise_connection_info(conn: pg_connection_type) -> str:
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

    return conn_info_str


class RoleInfo:
    def __init__(self, rolename: str, is_super_user: bool, can_login: bool, oid: int):
        self.name = rolename
        self.is_super_user = is_super_user
        self.can_login = can_login
        self.oid = oid

    # https://www.postgresql.org/docs/15/view-pg-roles.html
    def read_role_info(conn: pg_connection_type) -> list[RoleInfo]:
        cur = conn.cursor()
        cur.execute("select rolname, rolsuper, rolcanlogin, oid from pg_roles order by rolname;")

        l = [RoleInfo(record[0], record[1], record[2], record[3]) for record in cur]
        # for record in cur:
        #    print(record)
        #    for i in range(0, len(record)):
        #        print(i, record[i], type(record[i]))

        return l

    @classmethod
    def summarise_roles(cls, roles_list: list[RoleInfo], current_role: str) -> str:
        s = ""
        unlisted_pg_roles_count = 0
        for role in roles_list:
            if role.name.startswith("pg"):
                if role.name in [current_role, "pg_read_all_data", "pg_write_all_data"]:
                    include_role = True
                elif role.can_login or role.is_super_user:
                    include_role = True
                else:
                    # Just count these pg_.. roles rather than listing them all.
                    unlisted_pg_roles_count += 1
                    include_role = False
            else:
                include_role = True
            if include_role:
                if role.name == current_role:
                    s += f"[black on white]{role.name}[/]"
                else:
                    s += f"{role.name}"
                if role.can_login:
                    s += " Login"
                if role.is_super_user:
                    s += " Super-user"
                s += "\n"

        if unlisted_pg_roles_count > 0:
            plural = "s" if unlisted_pg_roles_count > 1 else ""
            s += f".. and {unlisted_pg_roles_count} other pg_xxx role{plural}"

        return s


class TablespaceInfo:
    def __init__(self, tablespace_name: str, owner_oid, owner: str, oid: int, size: int):
        self.name = tablespace_name
        self.owner_oid = owner_oid
        self.owner = owner
        self.oid = oid
        self.size = size
        self.file_location = "[yellow on red]????[/]"

    # https://www.postgresql.org/docs/15/catalog-pg-tablespace.html
    # https://www.postgresql.org/docs/15/functions-admin.html
    # https://www.postgresql.org/docs/15/functions-info.html
    # Could also include info from pg_tablespace_location and pg_tablespace_databases functions
    # Also see psql \db+ command
    # Location seems to be blank (=default location?) in basic installed database.
    # ??? How to query the file location information ?
    def read_tablespace_info(conn: pg_connection_type) -> list[TablespaceInfo]:
        cur = conn.cursor()
        query = """select t.spcname, t.spcowner, r.rolname, t.oid, pg_tablespace_size(t.oid)
                from pg_tablespace t join pg_roles r on t.spcowner = r.oid
                order by t.spcname;"""
        cur.execute(query)

        l = [
            TablespaceInfo(record[0], record[1], record[2], record[3], record[4]) for record in cur
        ]

        return l

    @classmethod
    def summarise_tablespaces(cls, ts_list: list[TablespaceInfo]) -> str:
        s = ""
        for ts in ts_list:
            size_in_mb: float = ts.size / 1024 / 1024
            s += f'{ts.name} : owner={ts.owner} : size={size_in_mb:.1f} MB : location="{ts.file_location}"\n'
        return s.strip()


class DatabaseInfo:
    def __init__(
        self,
        database_name: str,
        owner_oid,
        owner: str,
        oid: int,
        encoding_id: int,
        encoding_name: str,
        default_tablespace: str,
    ):
        self.name = database_name
        self.owner_oid = owner_oid
        self.owner = owner
        self.oid = oid
        self.encoding_id = encoding_id
        self.encoding_name = encoding_name
        self.default_tablespace = default_tablespace
        self.schemas_list: list[SchemaInfo] = []

    def set_schema_list(self, schemas_list: list[SchemaInfo]):
        self.schemas_list = schemas_list

    # https://www.postgresql.org/docs/current/catalog-pg-database.html
    # https://www.postgresql.org/docs/current/functions-info.html#PG-ENCODING-TO-CHAR
    # A 'Catalog' in SQL Standard terms.
    def read_database_info(conn: pg_connection_type) -> list[DatabaseInfo]:
        cur = conn.cursor()
        query = """select db.datname, db.datdba, r.rolname, db.oid, db.encoding, pg_encoding_to_char(db.encoding), ts.spcname
                from pg_database db 
		        join pg_roles r on db.datdba = r.oid
		        join pg_tablespace ts on db.dattablespace = ts.oid
                order by db.datname;"""
        cur.execute(query)

        l = [
            DatabaseInfo(
                record[0], record[1], record[2], record[3], record[4], record[5], record[6]
            )
            for record in cur
        ]

        return l

    @classmethod
    def summarise_databases(cls, db_list: list[DatabaseInfo], current_db) -> str:
        s = ""
        for db in db_list:
            if db.name == current_db:
                s += f"[black on white]{db.name}[/]"
            else:
                s += f"{db.name}"
            s += f" : owner={db.owner} : encoding={db.encoding_name} : tablespace={db.default_tablespace}\n"
        return s.strip()


class SchemaInfo:
    standard_pg_schema_descriptions: dict[str, str] = {
        "information_schema": "SQL Standard metadata",  # see https://www.postgresql.org/docs/15/information-schema.html"
        "pg_catalog": "Postgres native metadata (e.g. pg_... tables)",
        "pg_toast": "Storage of extended/large objects",
        "public": "Default schema",
    }

    def __init__(
        self,
        schema_name: str,
        owner_oid: int,
        owner: str,
        oid: int,
    ):
        self.name = schema_name
        self.owner_oid = owner_oid
        self.owner = owner
        self.oid = oid
        self.tables_list: list[TableInfo] = []
        self.views_list: list[ViewInfo] = []
        self.indexes_list: list[IndexInfo] = []

    # AKA namespace
    # https://www.postgresql.org/docs/15/ddl-schemas.html
    # https://www.postgresql.org/docs/current/catalog-pg-namespace.html
    def read_schema_info(conn: pg_connection_type) -> list[SchemaInfo]:
        cur = conn.cursor()
        query = """select ns.nspname, ns.nspowner, r.rolname, ns.oid
                from pg_namespace ns
		        join pg_roles r on ns.nspowner = r.oid
                order by ns.nspname;"""
        cur.execute(query)

        l = [SchemaInfo(record[0], record[1], record[2], record[3]) for record in cur]

        for si in l:
            si.tables_list = TableInfo.read_table_info_for_schema(conn, si.name)
            si.views_list = ViewInfo.read_view_info_for_schema(conn, si.name)
            si.indexes_list = IndexInfo.read_index_info_for_schema(conn, si.name)

        return l

    @classmethod
    def summarise_schemas(cls, sc_list: list[SchemaInfo], basics: BasicSessionInfo) -> str:
        s = ""
        for sc in sc_list:
            description = SchemaInfo.standard_pg_schema_descriptions.get(sc.name, "-")
            counts_info = f"tables={len(sc.tables_list)} views={len(sc.views_list)} indexes={len(sc.indexes_list)}"
            s += f'{sc.name} : owner={sc.owner} : description = "{description}" : {counts_info}\n'
        s += f"\nSearch path is : {basics.search_path}\n"
        s += f"Effective search path is : {basics.effective_search_path_list}"
        return s


class TableInfo:
    def __init__(self, schema_name: str, name: str, owner: str, tablespace_name: str):
        self.schema_name = schema_name
        self.name = name
        self.owner = owner
        self.tablespace_name = tablespace_name
        self.column_list: list[ColumnInfo] = []
        self.index_list: list[IndexInfo] = []

    # https://www.postgresql.org/docs/15/view-pg-tables.html
    def read_table_info_for_schema(conn: pg_connection_type, schema_name: str) -> list[TableInfo]:
        cur = conn.cursor()
        query = f"""select tablename, tableowner, tablespace
                from pg_tables
                where schemaname = '{schema_name}'
                order by tablename;"""
        cur.execute(query)

        l = [TableInfo(schema_name, record[0], record[1], record[2]) for record in cur]

        for ti in l:
            ti.column_list = ColumnInfo.read_column_info_for_table(conn, schema_name, ti.name)
            ti.index_list = IndexInfo.read_index_info_for_table(conn, schema_name, ti.name)

        return l

    @classmethod
    def summarise_tables(cls, ti_list: list[TableInfo]) -> str:
        s = ""
        for ti in ti_list:
            s += f"{ti.name} : owner={ti.owner} : columns={len(ti.column_list)} : indexes={len(ti.index_list)}\n"
        return s.strip()


class ViewInfo:
    def __init__(self, schema_name: str, name: str, owner: str, definition: str):
        self.schema_name = schema_name
        self.name = name
        self.owner = owner
        self.definition = definition

    # https://www.postgresql.org/docs/15/view-pg-views.html
    def read_view_info_for_schema(conn: pg_connection_type, schema_name: str) -> list[ViewInfo]:
        cur = conn.cursor()
        query = f"""select viewname, viewowner, definition
                from pg_views
                where schemaname = '{schema_name}'
                order by viewname;"""
        cur.execute(query)

        l = [ViewInfo(schema_name, record[0], record[1], record[2]) for record in cur]

        return l


class IndexInfo:
    def __init__(
        self, schema_name: str, name: str, table_name: str, tablespace_name: str, definition: str
    ):
        self.schema_name = schema_name
        self.name = name
        self.table_name = table_name
        self.tablespace_name = tablespace_name
        self.definition = definition

    # https://www.postgresql.org/docs/15/view-pg-indexes.html
    def read_index_info_for_schema(conn: pg_connection_type, schema_name: str) -> list[IndexInfo]:
        cur = conn.cursor()
        query = f"""select indexname, tablename, tablespace, indexdef
                from pg_indexes
                where schemaname = '{schema_name}'
                order by indexname;"""
        cur.execute(query)

        l = [IndexInfo(schema_name, record[0], record[1], record[2], record[3]) for record in cur]

        return l

    def read_index_info_for_table(
        conn: pg_connection_type, schema_name: str, table_name: str
    ) -> list[IndexInfo]:
        cur = conn.cursor()
        query = f"""select indexname, tablespace, indexdef
                from pg_indexes
                where schemaname = '{schema_name}'
                and tablename = '{table_name}'
                order by indexname;"""
        cur.execute(query)

        l = [IndexInfo(schema_name, record[0], table_name, record[1], record[2]) for record in cur]

        return l


class ColumnInfo:
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        name: str,
        type: str,
        length: int,
        not_null: bool,
        ordering: int,
    ):
        self.schema_name = schema_name
        self.table_name = table_name
        self.name = name
        self.type = type
        self.length = length
        self.not_null = not_null
        self.ordering = ordering

    # https://www.postgresql.org/docs/15/catalog-pg-attribute.html provides column info.
    def read_column_info_for_table(
        conn: pg_connection_type, schema_name: str, table_name: str
    ) -> list[ColumnInfo]:
        cur = conn.cursor()
        query = f"""select a.attname, typ.typname, a.attlen, a.attnotnull, a.attnum
                from pg_attribute a
                join pg_class c on a.attrelid = c.oid 
                join pg_type typ on a.atttypid = typ.oid
                join pg_namespace nam on c.relnamespace = nam.oid
                where nam.nspname = '{schema_name}'
                and c.relname = '{table_name}'
                order by a.attnum;"""

        cur.execute(query)

        l = [
            ColumnInfo(
                schema_name, table_name, record[0], record[1], record[2], record[3], record[4]
            )
            for record in cur
        ]

        return l


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
            conn.cursor().execute("Set Role pg_read_all_data")

        # Type-checking syntax restriction. Would like to do something like this:
        #   (basics, basics_str) : tuple[BasicDBInfo, str] = read_basics(conn)
        # but it's not allowed by Python typing spec.
        # If we do just:
        #   basics, basics_str = read_basics(conn)
        # then the variables end up with type 'Any'
        result: tuple[BasicSessionInfo, str] = read_session_basics(conn)
        basics: BasicSessionInfo = result[0]
        basics_str: str = result[1]

        panel = rich.panel.Panel(basics_str, title="Session info")
        console.print(panel)

    # Show info about the connection
    conn_info_str = summarise_connection_info(conn)
    panel = rich.panel.Panel(conn_info_str, title="Connection info")
    console.print(panel)

    roles_list = RoleInfo.read_role_info(conn)
    roles_info_str = RoleInfo.summarise_roles(roles_list, basics.current_user)
    panel = rich.panel.Panel(roles_info_str, title="Roles info")
    console.print(panel)

    ts_list = TablespaceInfo.read_tablespace_info(conn)
    ts_info_str = TablespaceInfo.summarise_tablespaces(ts_list)
    panel = rich.panel.Panel(ts_info_str, title="Tablespace info")
    console.print(panel)

    db_list = DatabaseInfo.read_database_info(conn)
    db_info_str = DatabaseInfo.summarise_databases(db_list, basics.current_database)
    panel = rich.panel.Panel(db_info_str, title="Database info")
    console.print(panel)

    sc_list = SchemaInfo.read_schema_info(conn)
    sc_info_str = SchemaInfo.summarise_schemas(sc_list, basics)
    panel = rich.panel.Panel(
        sc_info_str, title=f'Schema info for the current database : "{basics.current_database}"'
    )
    console.print(panel)

    # Attach the schema list to the info for the current database
    current_db: Optional[DatabaseInfo] = None
    for db in db_list:
        if db.name == basics.current_database:
            current_db = db
            break

    if current_db is not None:
        current_db.set_schema_list(sc_list)
    else:
        console.print(f"[red]** failed to identify the current database {basics.current_database}")

    # target_schema = "pg_catalog"
    for sc in sc_list:
        if len(sc.tables_list) > 0:
            tables_info_str = TableInfo.summarise_tables(sc.tables_list)
            panel = rich.panel.Panel(tables_info_str, title=f"Tables in schema {sc.name}")
            console.print(panel)

    produce_tree(basics, roles_list, ts_list, db_list)


def produce_tree(
    basics: BasicSessionInfo,
    roles_list: list[RoleInfo],
    ts_list: list[TablespaceInfo],
    db_list: list[DatabaseInfo],
):
    my_tree = rich.tree.Tree(f"Postgres Cluster")

    system_tree = my_tree.add("System:")
    system_tree.add(f"Cluster name: {basics.cluster_name}")
    system_tree.add(f"Postgres version: {basics.pg_version}")

    session_tree = my_tree.add("This session:")
    session_tree.add(f"Current user: {basics.current_user}")
    session_tree.add(f"Current database: {basics.current_database}")
    session_tree.add(f"Search path: {basics.search_path}")
    session_tree.add(f"Effective search path: {basics.effective_search_path_list}")

    roles_tree = my_tree.add(f"Roles ({len(roles_list)}):")
    for role in roles_list[0:2]:
        roles_tree.add(role.name)
    ts_tree = my_tree.add(f"Tablespaces ({len(ts_list)}):")
    for ts in ts_list:
        ts_tree.add(ts.name)
    dbs_tree = my_tree.add(f"Databases ({len(db_list)}):")
    for db in db_list:
        if db.name != basics.current_database:
            db_tree = dbs_tree.add(f"{db.name} : <not visible from this session>")
        else:
            db_tree = dbs_tree.add(db.name)
            schemas_tree = db_tree.add(f"Schemas ({len(db.schemas_list)}):")
            for schema in db.schemas_list:
                schema_tree = schemas_tree.add(schema.name)
                tables_tree = schema_tree.add(f"Tables ({len(schema.tables_list)}):")
                # for t in schema.tables_list:
                #    table_tree = tables_tree.add(t.name)
                views_tree = schema_tree.add(f"Views ({len(schema.views_list)}):")
                # for v in schema.views_list:
                #    views_tree.add(v.name)
                indexes_tree = schema_tree.add(f"Indexes ({len(schema.indexes_list)}):")
    panel = rich.panel.Panel(my_tree, title=f"Summary tree")
    console.print(panel)


if __name__ == "__main__":
    main()
