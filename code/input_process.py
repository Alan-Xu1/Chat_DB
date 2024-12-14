import spacy
from sqlalchemy.sql import column
from sqlalchemy import create_engine, MetaData, Table, select, and_, or_, desc, text, inspect
import pymysql
import random

class input_process:

    def __init__(self, db_name):
        self.summary = {
            'average': 'AVG',
            'mean': 'AVG',
            'sum': 'SUM',
            'total': 'SUM',
            'count': 'COUNT',
            'minimum': 'MIN',
            'maximum': 'MAX',
            'min': 'MIN',
            'max': 'MAX'
        }

        self.comparison = {
            'greater than': '>',
            'larger than': '>',
            'more than': '>',
            'above': '>',
            'less than ': '<',
            'smaller than': '<',
            'below': '<',
            'equal': '=',
            'equals': '=',
            'is': '=',
            'not equal': '!=',
            'greater or equal': '>=',
            'less or equal': '<=',
        }

        self.keywords = {
            'show': 'SELECT',
            'find': 'SELECT',
            'get': 'SELECT',
            'select': 'SELECT',
        }

        self.connection_parameters = {
            "host": "chatdb.clwu228s6zcd.us-west-2.rds.amazonaws.com",
            "user": "admin",
            "password": "DSCI-551",
            "port": 3306
        }
        self.db_name = db_name
        self.base_connection = f"mysql+pymysql://{self.connection_parameters['user']}:{self.connection_parameters['password']}@{self.connection_parameters['host']}:{self.connection_parameters['port']}"
        self.nlp = spacy.load('en_core_web_sm')
        self.engine = create_engine(f"{self.base_connection}/{db_name}")

    def get_table_name(self,db_name):
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names

    def nl_sql(self, query, table, column):
        try:
            if query.lower() == 'random query':
                table_match = random.choice(table)
                column_match = random.choice(column[table_match])
                with self.engine.connect() as connection:
                    connection.execute(text(f'USE {self.db_name}'))
                    connection.commit()
                sql = f"SELECT {column_match} FROM {table_match}".strip()
                result = ""
                with self.engine.connect() as connection:
                    temp = connection.execute(text(sql))
                    result = temp.fetchall()
                return [sql, result]
            for i in range(len(table)):
                table[i] = table[i].lower()
            query = query.lower().strip()
            doc = self.nlp(query)
            token_list = []
            for token in doc:
                token_list.append(token.text)
            operation = ""
            for i in range(len(token_list)):
                token_list[i] = token_list[i].lower()
            # the query must start with either show, find, get or select
            for key,value in self.keywords.items():
                if token_list[0] == key:
                    operation = value
                    break

            if operation == "":
                return ('Error',
                        "Sorry I cannot do what you ask of me yet... Remember to start query with show, find, get, select, select.")

            # currently table name must be after from or in
            # and only one table could be queried at a time
            table_match = ""
            for i in range(len(token_list)):
                if token_list[i] == "from" or token_list[i] == "in":
                    table_match = token_list[i+1]
                    break
            if table_match == "":
                return ('Error', "Did you put the table name after 'in' or 'from'? :)")
            if table_match not in table:
                return ('Error', "Sorry I cannot find a table to search for...")


            # for things like find X of Y
            # we only find the first operation that match
            summary_match = ""
            if operation != "":
                for key, value in self.summary.items():
                    for i in range(len(token_list)):
                        if token_list[i] == key:
                            summary_match = f"{value}({token_list[i+2]})"
                            break

            # I decide to display all columns in the query
            columns = ""
            cols_match = set()
            if summary_match == "":
                columns = "*"
                valid_column = column[table_match]
                for i in range(len(valid_column)):
                    valid_column[i] = valid_column[i].lower()
                for i in range(len(token_list)):
                    if token_list[i] in valid_column:
                        cols_match.add(token_list[i])

            if cols_match:
                columns = ", ".join(cols_match)

            # where clause conditions
            # we need something like where column_name (operator)
            where = ""
            # let's check for three word operator first
            # this is a greedy approach
            found_match = False
            valid_column = column[table_match]
            for i in range(len(valid_column)):
                valid_column[i] = valid_column[i].lower()
            for i in range(len(token_list)):
                if i+3 < len(token_list):
                    multi = f'{token_list[i]} {token_list[i+1]} {token_list[i+2]}'
                    if multi in self.comparison.keys():
                        if token_list[i-1] in valid_column:
                            where = f"WHERE {token_list[i-1]} {self.comparison[multi]} {token_list[i+3]}"
                            found_match = True
                            break

            # now we check for two word operator
            if not found_match:
                for i in range(len(token_list)):
                    if i+2 < len(token_list):
                        two = f'{token_list[i]} {token_list[i+1]}'
                        if two in self.comparison.keys():
                            if token_list[i - 1] in valid_column:
                                where = f'WHERE {token_list[i-1]} {self.comparison[two]} {token_list[i+2]}'
                                found_match = True
                                break

            # now we check for one word operator
            if not found_match:
                for i in range(len(token_list)):
                    if i+1 < len(token_list):
                        if token_list[i] in self.comparison.keys():
                            if token_list[i - 1] in valid_column:
                                where = f'WHERE {token_list[i-1]} {self.comparison[token_list[i]]} {token_list[i+1]}'
                                break

            # let's do the groupby operation
            # group by X
            # it only make sense if we have 2 or more columns

            group = ''
            if len(cols_match) > 1:
                for i in range(len(token_list)):
                    if i+2 < len(token_list):
                        group_by = f'{token_list[i]} {token_list[i+1]}'
                        if group_by == 'group by':
                            if token_list[i+2] in cols_match:
                                group = f'GROUP BY {token_list[i+2]}'
                                break

            # now we can add the final order by
            order = ''
            if group != '':
                for i in range(len(token_list)):
                    if i+2 < len(token_list):
                        order_by = f'{token_list[i]} {token_list[i+1]}'
                        if order_by == 'order by':
                            if token_list[i+2] in cols_match:
                                order = f'ORDER BY {token_list[i+2]}'
                                break

            # generate the result
            with self.engine.connect() as connection:
                connection.execute(text(f'USE {self.db_name}'))
                connection.commit()
            sql = f"SELECT {summary_match} {columns} FROM {table_match} {where} {group} {order}".strip()
            result = ""
            with self.engine.connect() as connection:
                temp = connection.execute(text(sql))
                result = temp.fetchall()
            return [sql, result]
        except Exception as e:
            return ('Error', 'Sorry restructure your query')





