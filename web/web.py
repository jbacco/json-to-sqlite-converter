#!/usr/bin/env python3

import sys
import os
from glob import glob
from flask import Flask, abort, render_template, jsonify, request
sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))  # Need this for the next import
from j2s import JsonToSqliteConverter, Db, Helpers

app = Flask(__name__)
app.config.update({'JSON_SORT_KEYS': False})

converter = JsonToSqliteConverter()


def get_database_directory():
    """
    Resolves the directory whose SQLite databases are shown in the web UI.

    :return: Absolute directory path.
    """
    return os.path.abspath(os.path.expanduser(os.environ.get('J2S_DATABASE_DIR') or os.getcwd()))


def get_database_files():
    """
    Finds SQLite databases in the web server database directory.

    :return: List of absolute database filepaths.
    """
    return sorted(os.path.abspath(path) for path in glob(os.path.join(get_database_directory(), '*.sqlite3')))


def get_environment_database():
    """
    Resolves the database configured when the web server started.

    :return: Absolute database filepath, or None.
    """
    env_database = Helpers.empty_to_none(os.environ.get('J2S_DATABASE', None))
    return converter.resolve_database_filepath(env_database) if env_database else None


def get_allowed_database_files():
    """
    Lists databases that can be selected by request parameters.

    :return: List of absolute database filepaths.
    """
    database_files = get_database_files()
    env_database = get_environment_database()

    if env_database and os.path.exists(env_database) and env_database not in database_files:
        database_files.insert(0, env_database)

    return database_files


def resolve_requested_database(db_param):
    """
    Resolves a request database parameter if it matches an allowed database.

    :param db_param: Database parameter from the request.
    :return: Absolute database filepath, or None.
    """
    db_filepath = converter.resolve_database_filepath(db_param)
    return db_filepath if db_filepath in get_allowed_database_files() else None


def get_database_filepath():
    """
    Resolves the active database path from the request or web server environment.

    :return: Absolute database filepath.
    """
    db_param = Helpers.empty_to_none(request.values.get('database', None))
    if db_param:
        return resolve_requested_database(db_param)

    env_database = get_environment_database()
    if env_database:
        return env_database

    default_database = os.path.join(get_database_directory(), 'db.sqlite3')
    if os.path.exists(default_database):
        return default_database

    database_files = get_database_files()
    if database_files:
        return database_files[0]

    return default_database


def get_database_choices(selected_database):
    """
    Lists dropdown database choices with the selected database first.

    :param selected_database: Currently selected database filepath.
    :return: List of dicts for the database dropdown.
    """
    selected_database = os.path.abspath(selected_database) if selected_database else None
    database_files = get_allowed_database_files()

    if selected_database in database_files:
        database_files.insert(0, database_files.pop(database_files.index(selected_database)))

    return [{'path': path, 'name': os.path.basename(path)} for path in database_files]


class DataTables:
    """
    Class used to handles actions related to a DataTable.
    """
    def __init__(self):
        self.length = 10
        self.start = 0
        self.draw = Helpers.to_int(request.values.get('draw', 1), 1)
        self.order_col_index = Helpers.to_int(request.values.get('order[0][column]', 0), 0) + 1
        self.direction = 'DESC' if request.values.get('order[0][dir]', 'ASC') == 'desc' else 'ASC'

    @staticmethod
    def get_table_config(database, tables=None):
        """
        Compiles a dict of tables mapped to their normalized columns.

        :param database: Database to use.
        :param tables: List of tables to use (if None, then all tables will be retrieved).
        :return: Dict of table name strings mapped to their comma-separated column name strings
        """
        return dict(map(lambda t: (t, ','.join(database.get_table_columns(t))), database.get_tables() if not tables else tables))

    def get_response(self, total_count, filtered_count, rows, error=None):
        """
        Returns a response for a DataTable AJAX query.

        :param rows: List of rows where each row is a dict in the form of {"column_name": value}.
        :param total_count: Total number of rows in the table.
        :param filtered_count: Total number of rows in the table after filtering.
        :param error: Error message to display (do not set if there's no error).
        :return: JSON object DataTables response.
        """
        return jsonify({
            'draw': self.draw,
            'recordsTotal': total_count,
            'recordsFiltered': filtered_count,
            'data': rows
        }) if not error else jsonify({
            'draw': self.draw,
            'recordsTotal': 0,
            'recordsFiltered': 0,
            'data': [],
            'error': error
        })


@app.route('/')
def global_search_page():
    """
    Global search page.  Accepts GET requests and both GET/POST parameters.

    :return: Rendered Jinja HTML template.
    """
    db_param = Helpers.empty_to_none(request.values.get('database', None))
    db_filepath = get_database_filepath()
    if not os.path.exists(db_filepath):
        abort(500, f'Database "{db_param if db_param else db_filepath}" not found.')

    db = Db(db_filepath)
    query = request.values.get('query', None)
    tables = DataTables.get_table_config(db)

    return render_custom_template('global_search.html', global_search_api='/api/v1/global-search/search', query=query, tables=tables)


@app.route('/api/<version>/<method>/<action>', methods=['GET', 'POST'])
def api(version, method, action):
    """
    API entry point.  Accepts both GET and POST requests/parameters.

    :param version: Version being requested.
    :param method: Method being requested.
    :param action: Action being requested.
    :return: JSON object API response.
    """
    if version == 'v1' and method == 'global-search' and action == 'search':
        return global_search()
    return jsonify({'error': 'Invalid request.'})


def render_custom_template(template, **kwargs):
    """
    Custom template renderer.  Ensures the base.html template has all its required arguments.

    :return: Rendered Jinja HTML template.
    """
    db_param = Helpers.empty_to_none(request.values.get('database', None))
    db_filepath = get_database_filepath()

    if not db_filepath or not os.path.exists(db_filepath):
        if db_param:
            abort(500, f'Database "{db_param}" not found.')
        else:
            abort(500, f'No databases found.')

    databases = get_database_choices(db_filepath)

    kwargs.update(databases=databases)

    return render_template(template, **kwargs)


def global_search():
    """
    API handler for DataTables JSON responses to global search requests.

    Request parameters:
        database: Database to use (required).
        table: Table to search (required).
        query: Query keyword to search for (default: None).
        order: Column index number by which to order the results (default: 1).
        direction: Direction to order the results (default: ASC).
        length: Maximum number of results to return (default: 10).
        start: Number by which to offset the results (default: 0).

    :return: JSON object DataTables response.
    """
    dt = DataTables()

    db_param = Helpers.empty_to_none(request.values.get('database', None))
    db_filepath = get_database_filepath()

    if not db_filepath or not os.path.exists(db_filepath):
        if db_param:
            return dt.get_response(0, 0, [], f'Database "{db_param}" not found.')
        else:
            return dt.get_response(0, 0, [], f'Must specify a database parameter.')

    db = Db(db_filepath)
    table = Helpers.empty_to_none(request.values.get('table', None))

    if not table:
        return dt.get_response(0, 0, [], f'No table specified.')
    if not db.table_exists(table):
        return dt.get_response(0, 0, [], f'Table "{table}" does not exist.')

    columns = DataTables.get_table_config(db, [table])[table].split(',')
    query = Helpers.empty_to_none(request.values.get('query', None))
    order = Helpers.empty_to_none(request.values.get('order', None))
    order = Helpers.to_int(order, dt.order_col_index)
    direction = Helpers.empty_to_none(request.values.get('direction', None))
    direction = direction if direction else dt.direction
    limit = Helpers.empty_to_none(request.values.get('length', None))
    limit = Helpers.to_int(limit, dt.length)
    offset = Helpers.empty_to_none(request.values.get('start', None))
    offset = Helpers.to_int(offset, dt.start)

    try:
        total_count, filtered_count, rows = db.search_table(table, columns, query, order, direction, limit, offset)
        return dt.get_response(total_count, filtered_count, rows)
    except Exception:
        return dt.get_response(0, 0, [], f"Error attempting to fetch data: Check your API request for references to tables or columns that don't exist.")
