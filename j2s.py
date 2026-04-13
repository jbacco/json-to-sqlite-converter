#!/usr/bin/env python3

import argparse
import datetime
import json
import os
import psutil
import re
import signal
import subprocess
import sys
from colored import attr, fg
from sqlite3 import Row, connect
from sqlite_utils import Database, suggest_column_types
from tabulate import tabulate


class Regex:
    FTS_TABLE = r'^.*(_fts|_fts_config|_fts_data|_fts_docsize|_fts_idx)$'


class Colors:
    APP = fg('deep_sky_blue_2')
    INFO = fg('deep_sky_blue_2')
    WARN = fg('yellow')
    SUCCESS = fg('green')
    FAIL = fg('magenta')
    RESET = attr('reset')


class Tags:
    INFO = f"{Colors.INFO}[*]{Colors.RESET}"
    WARN = f"{Colors.WARN}[!]{Colors.RESET}"
    SUCCESS = f"{Colors.SUCCESS}[+]{Colors.RESET}"
    FAIL = f"{Colors.FAIL}[-]{Colors.RESET}"


class Helpers:
    @staticmethod
    def empty_to_none(value):
        return None if not value else value

    @staticmethod
    def to_int(value, default):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def strip_filename(filename):
        return os.path.splitext(os.path.basename(filename))[0]

    @staticmethod
    def normalize_table_name(name):
        return re.sub(r'_+', '_', re.sub(r'[^a-zA-Z0-9_]', '_', name)).strip('_').lower()

    @staticmethod
    def normalize_row(row):
        return {Helpers.normalize_table_name(key): value for key, value in row.items()}

    @staticmethod
    def timestamp_to_human_datetime(timestamp):
        return datetime.datetime.fromtimestamp(float(timestamp)).astimezone().strftime('%a, %b %d, %Y %I:%M %p %Z')


class Db:
    def __init__(self, filepath):
        assert filepath, 'No database specified.'
        assert os.path.exists(filepath), f'Database {filepath} not found.'
        self.db_path = filepath
        self.db = Database(connect(self.db_path))

    @staticmethod
    def not_fts_table(table):
        return False if re.match(Regex.FTS_TABLE, table) else True

    @staticmethod
    def normalize_fts_query(query):
        quoted_phrases = re.findall(r'"([^"]*)"', query)
        phrases = [phrase.strip() for phrase in quoted_phrases if phrase.strip()]

        query = re.sub(r'"[^"]*"', ' ', query)

        keywords = re.findall(r'\w+', query)
        keywords = [keyword for keyword in keywords if keyword.upper() not in ['AND', 'OR', 'NOT', 'NEAR']]
        keyword_queries = [f'{keyword}*' for keyword in keywords]
        phrase_queries = [f'"{phrase}"' for phrase in phrases]

        return ' OR '.join(phrase_queries + keyword_queries) if phrase_queries or keyword_queries else None

    @staticmethod
    def quote_identifier(identifier):
        return '"' + str(identifier).replace('"', '""') + '"'

    def get_tables(self, filter_fts=True):
        return list(filter(Db.not_fts_table, self.db.table_names())) if filter_fts else self.db.table_names()

    def get_table_columns(self, table):
        return [name for name in self.db[table].columns_dict.keys()]

    def table_exists(self, table):
        return self.db[table].exists()

    def column_exists(self, table, column):
        return self.table_exists(table) and column in self.db[table].columns_dict.keys()

    def search_table(self, table, columns, query, order=1, direction='ASC', limit=0, offset=0):
        assert self.table_exists(table), f'Table {table} not found.'

        if columns:
            for column in columns:
                assert self.column_exists(table, column), f'Column {column} not found.'
        else:
            columns = self.get_table_columns(table)

        order = int(order)
        order = order if 1 <= order <= len(columns) else 1
        direction = 'DESC' if direction.lower() in ['desc', 'descending'] else 'ASC'
        limit = int(limit)
        limit_sql = f' LIMIT {limit}' if limit else ''
        offset = int(offset)
        offset_sql = f' OFFSET {offset}' if offset else ''

        self.db.conn.row_factory = Row
        total_count = self.db.table(table).count
        query = Db.normalize_fts_query(query) if query else None
        match = '{' + ' '.join(columns) + '}: ' + f'{query}' if query else None
        quoted_table = Db.quote_identifier(table)
        quoted_fts_table = Db.quote_identifier(f'{table}_fts')
        quoted_columns = ','.join(Db.quote_identifier(column) for column in columns)

        if match:
            c = self.db.conn.execute(f"SELECT COUNT(*) AS filtered_count FROM {quoted_table} WHERE rowid IN (SELECT rowid FROM {quoted_fts_table} WHERE {quoted_fts_table} MATCH ?)", (match,))
            filtered_count = c.fetchone()['filtered_count']
            c = self.db.conn.execute(f"SELECT {quoted_columns} FROM {quoted_table} WHERE rowid IN (SELECT rowid FROM {quoted_fts_table} WHERE {quoted_fts_table} MATCH ?) ORDER BY {order} {direction}{limit_sql}{offset_sql}", (match,))
        else:
            filtered_count = total_count
            c = self.db.conn.execute(f"SELECT {quoted_columns} FROM {quoted_table} ORDER BY {order} {direction}{limit_sql}{offset_sql}")

        return total_count, filtered_count, [dict(row) for row in c.fetchall()]


class JsonToSqliteConverter:
    def __init__(self):
        self.web_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'web')
        self.web_script = os.path.join(self.web_dir, 'web.py')

    @staticmethod
    def resolve_database_filepath(database=None):
        return os.path.abspath(os.path.expanduser(database if database else 'db.sqlite3'))

    def _find_webserver_pid(self):
        for process in psutil.process_iter():
            try:
                for line in process.cmdline():
                    if 'flask' in line and self.web_script == process.environ().get('FLASK_APP', None):
                        return process.pid
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue

        return None

    def _normalize_json_rows(self, data):
        rows = data if isinstance(data, list) else [data]
        return [Helpers.normalize_row(row) if isinstance(row, dict) else {'value': row} for row in rows]

    def import_json_file(self, json_file, database=None):
        json_file = os.path.abspath(os.path.expanduser(json_file))
        if not os.path.exists(json_file):
            sys.exit(f'{Tags.FAIL} JSON file {Colors.INFO}{json_file}{Colors.RESET} was not found.')

        db_filepath = self.resolve_database_filepath(database)
        table_name = Helpers.normalize_table_name(f"{Helpers.strip_filename(json_file)}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}")

        with open(json_file, 'r') as f:
            rows = self._normalize_json_rows(json.load(f))

        if not rows:
            sys.exit(f'{Tags.FAIL} JSON file {Colors.INFO}{json_file}{Colors.RESET} does not contain any rows to import.')

        db_dir = os.path.dirname(db_filepath)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        database = Database(connect(db_filepath))
        table = database.table(table_name)
        table.create(suggest_column_types(rows))
        table.insert_all(rows, alter=True)
        for column in table.columns_dict.keys():
            table.create_index([column])
        table.enable_fts([name for name in table.columns_dict.keys()], create_triggers=True)

        print(f'{Tags.SUCCESS} Imported {Colors.INFO}{len(rows):n}{Colors.RESET} row(s) into {Colors.INFO}{db_filepath}{Colors.RESET} as table {Colors.INFO}{table_name}{Colors.RESET}.')

    def status(self, database=None):
        db_filepath = self.resolve_database_filepath(database)
        print(f'{Tags.INFO} Database: {Colors.INFO}{db_filepath}{Colors.RESET}')

        if os.path.exists(db_filepath):
            db_updated = Helpers.timestamp_to_human_datetime(os.path.getmtime(os.path.realpath(db_filepath)))
            db_size = os.stat(os.path.realpath(db_filepath)).st_size
            db = Database(connect(db_filepath))
            db_table = [
                ['last_updated:', f'{Colors.INFO}{db_updated}{Colors.RESET}'],
                ['size:', f'{Colors.INFO}{db_size:n}{Colors.RESET} byte(s)']
            ]

            for table in db.table_names():
                if not re.match(Regex.FTS_TABLE, table):
                    columns = ', '.join(db[table].columns_dict.keys())
                    rowcount = db[table].count if db[table].exists() else 0
                    db_table.append([f'{table}:', f'{Colors.INFO}{rowcount:n}{Colors.RESET} row(s); {Colors.INFO}{columns}{Colors.RESET}'])
        else:
            db_table = [['status:', f'{Colors.WARN}not found{Colors.RESET}']]

        print(tabulate(db_table, tablefmt='plain'))
        print(f'\n{Tags.INFO} Web server:')
        web_pid = self._find_webserver_pid()
        web_table = [['status:', f'{Colors.SUCCESS}active{Colors.RESET}' if web_pid else f'{Colors.FAIL}inactive{Colors.RESET}']]
        if web_pid:
            web_table.append(['address:', f'{Colors.INFO}http://127.0.0.1:5000/{Colors.RESET}'])
            web_table.append(['pid:', f'{Colors.INFO}{web_pid}{Colors.RESET}'])
        print(tabulate(web_table, tablefmt='plain'))

    def web_start(self, database=None):
        if self._find_webserver_pid():
            sys.exit(f'{Tags.WARN} Web server appears to be running. Check {Colors.INFO}http://127.0.0.1:5000/{Colors.RESET} or try stopping it with {Colors.INFO}j2s web stop{Colors.RESET} and then run this command again.')

        env = os.environ.copy()
        env.update({
            'FLASK_APP': self.web_script,
            'FLASK_ENV': 'development',
            'FLASK_DEBUG': '0',
            'J2S_DATABASE': self.resolve_database_filepath(database) if database else '',
            'J2S_DATABASE_DIR': os.getcwd()
        })
        subprocess.Popen(
            ['python' if os.name == 'nt' else 'python3', '-m', 'flask', 'run'],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            shell=True if os.name == 'nt' else False
        )
        print(f'{Tags.SUCCESS} Web server listening on {Colors.INFO}http://127.0.0.1:5000/{Colors.RESET}')

    def web_stop(self):
        pid = self._find_webserver_pid()
        if pid:
            os.kill(pid, signal.SIGINT)
            print(f'{Tags.SUCCESS} Web server stopped.')
            return

        print(f'{Tags.WARN} Web server does not appear to be running.')


def _parse_args():
    parser = argparse.ArgumentParser(
        prog='j2s',
        description='JSON to SQLite database converter',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        usage='''j2s <file.json> [--database <database>]
       j2s status [--database <database>]
       j2s web start [--database <database>]
       j2s web stop''',
        epilog='''commands:
  file.json   Add a JSON file as a new indexed table in the database
  status      View database stats and table structure
  web start   Start the web server
  web stop    Stop the web server'''
    )
    parser.add_argument('command', type=str, default=None, nargs='+', help='file.json, status, web start, or web stop')
    parser.add_argument('--database', dest='database', type=str, default=None, metavar='database', help='SQLite database path (default: db.sqlite3)')
    return parser.parse_args()


def _main():
    args = _parse_args()
    converter = JsonToSqliteConverter()
    command = ' '.join(args.command)

    if command == 'status':
        converter.status(args.database)
    elif command == 'web start':
        converter.web_start(args.database)
    elif command == 'web stop':
        if args.database:
            sys.exit(f'{Tags.FAIL} j2s web stop does not accept {Colors.INFO}--database{Colors.RESET}.')
        converter.web_stop()
    elif len(args.command) == 1:
        converter.import_json_file(args.command[0], args.database)
    else:
        sys.exit(f'{Tags.FAIL} Command not found. Try {Colors.INFO}j2s --help{Colors.RESET} for a list of commands.')


if __name__ == '__main__':
    _main()
