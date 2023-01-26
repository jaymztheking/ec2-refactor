import datetime as dt

import snowflake.connector
import psycopg2

#Loose Interfaces to list necessary methods
class Source:
    def __init__(self, config: dict):
        pass

    def connect(self) -> bool:
        pass

    def disconnect(self) -> bool:
        pass

    def build_incremental_load_query(self, schema: str, table: str, audit_cols: list, latest_date: dt.datetime) -> str:
        pass

    def build_full_load_query(self, schema: str, table: str, audit_cols: list, method: str ='Insert') -> str:
        pass

    def get_row_count(self, schema: str, table: str) -> int:
        pass

    def run_select_query(self, query: str, num_records_per_fetch: int) -> list:
        pass

class Target:
    def __init__(self, config: dict):
        pass

    def connect(self) -> bool:
        pass

    def disconnect(self) -> bool:
        pass

    def truncate(self, schema: str, table: str) -> bool:
        pass

    def run_script(self, scriptfilepath: str) -> bool:
        pass

    def get_row_count(self, table: str) -> int:
        pass

    def get_latest_date_in_table(self, schema: str, table: str, audit_cols: list =None) -> dt.datetime:
        pass

#Implementations of Sources and Targets
class Oracle(Source):
    pass


class Postgres(Source):
    def __init__(self, config):
        self.dbname = config['dbname']
        self.user = config['user']
        self.password = config['password']
        self.host = config['host']
        self.port = config['port']
        self.connection = None

    def connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)
            self.cursor= self.connection.cursor()
            return True
        except psycopg2.OperationalError as err:
            print(err)
            return False

    def disconnect(self) -> bool:
        if self.connection is not None:
            try:
                self.connection.close()
                self.connection = None
                self.cursor = None
                return True
            except Exception as err:
                print(err)
                return False
        else:
            return False

    def build_incremental_load_query(self, schema: str, table: str, audit_cols: list, latest_date: dt.datetime) -> str:
        query = "SELECT * FROM " + schema + "." + table + " WHERE "
        for col in audit_cols:
            column_condition = (
                f"(date_trunc('day', {col})>TO_DATE('{latest_date}','YYYY-MM-DD') "
                f"AND date_trunc('day', {col})<=(CURRENT_DATE - INTERVAL '1 day'))"
            )
            if not col == audit_cols[-1]:
                join_condition = " OR "
            else:
                join_condition = ""
            query = query + column_condition + join_condition
        return query

    def build_full_load_query(self, schema: str, table: str, audit_cols: list, method: str ='Insert') -> str:
        if method == 'Insert':
            query = 'SELECT * FROM ' + schema + "." + table + " WHERE "
            for col in audit_cols:
                column_condition = (
                    f"date_trunc('day', {col})<=(CURRENT_DATE - INTERVAL '1 day')"
                )
                if not col == audit_cols[-1]:
                    join_condition = " OR "
                else:
                    join_condition = ""
                query = query + column_condition + join_condition
        else: #For copy statement method in future versions
            query = 'COPY bla bla bla'
        return query

    def build_partial_load_query(self, table: str, audit_cols: list, start_date: dt.datetime) -> str:
        pass

    def get_row_count(self, schema: str, table: str) -> int:
        standalone_run = False
        query = f"SELECT COUNT(*) FROM {schema}.{table};"
        try:
            if self.connection is None:
                standalone_run = True
                self.connect()
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if standalone_run:
                self.disconnect()
            if len(result) == 1:
                count = result[0]
                return count
        except Exception as err:
            print(err)
            return -1
            

    def run_select_query(self, query: str, num_records_per_fetch: int) -> list:
        standalone_run = False
        #when being ran standalone
        if self.connection is None:
            standalone_run = True
            self.connect()

        #query has not been run yet
        if self.cursor.query is None:
            self.cursor.execute(query)
        results = self.cursor.fetchmany(num_records_per_fetch)
        if results == []:
            self.cursor.close()
            if standalone_run:
                self.disconnect()
            return None
        return results

class Snowflake(Target):
    def __init__(self, config: dict):
        self.user = config['user']
        self.password = config['password']
        self.account = config['account']
        self.warehouse = config['warehouse']
        self.database = config['database']
        self.role = config['role']
        self.connection = None

    def connect(self) -> bool:
        try:
            self.connection = snowflake.connector.connect(  user=self.user, 
                                                            password=self.password, 
                                                            account=self.account, 
                                                            warehouse=self.warehouse, 
                                                            database=self.database, 
                                                            role=self.role)
            self.cursor = self.connection.cursor()
            return True
        except:
            return False
        
    def disconnect(self) -> bool:
        if self.connection is not None:
            try:
                self.connection.close()
                self.connection = None
                self.cursor = None
                return True
            except Exception as err:
                print(err)
                return False
        else:
            return False

    def truncate(self, schema: str, table: str) -> bool:
        query = f"TRUNCATE TABLE {schema}.{table}"
        standalone_run = False
        try:
            if self.connection is None:
                standalone_run = True
                self.connect()   
            self.cursor.execute(query)
            if standalone_run:
                self.disconnect()
            return True
        except Exception as err:
            print(err)
            return False

    def run_script(self, scriptfilepath: str) -> bool:
        standalone_run = False
        print(scriptfilepath)
        with open(scriptfilepath) as script:
            query = script.read()
        try:
            if self.connection is None:
                standalone_run = True
                self.connect()   
            self.cursor.execute(query)
            if standalone_run:
                self.disconnect()
            return True
        except Exception as err:
            print(err)
            return False

    def get_row_count(self, schema: str, table: str) -> int:
        standalone_run = False
        query = f"SELECT COUNT(*) FROM {schema}.{table};"
        try:
            if self.connection is None:
                standalone_run = True
                self.connect()
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if standalone_run:
                self.disconnect()
            if len(result) == 1:
                count = result[0]
                return count
        except Exception as err:
            print(err)
            return -1

    def get_latest_date_in_table(self, schema: str, table: str, audit_cols: list =None) -> dt.datetime:
        standalone_run = False
        query = f"SELECT MAX(max_date_loaded) FROM {schema}.{table} " \
                f"UNPIVOT (max_date_loaded FOR nDate IN ({','.join(audit_cols)}))"
        try:
            if self.connection is None:
                standalone_run = True
                self.connect()
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if standalone_run:
                self.disconnect()
            if len(result) == 1:
                count = result[0]
                return count
        except Exception as err:
            print(err)
            return -1

