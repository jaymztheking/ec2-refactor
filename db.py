import datetime as dt
import snowflake.connector

#Loose Interfaces to list necessary methods
class Source:
    def __init__(self, config: dict):
        pass

    def connect(self) -> bool:
        pass

    def disconnect(self) -> bool:
        pass

    def build_incremental_load_query(self, table: str, audit_cols: list, latest_date: dt.datetime) -> str:
        pass

    def build_full_load_query(self, table: str, audit_cols: list, latest_date: dt.datetime, method: str ='Insert') -> str:
        pass

    def build_partial_load_query(self, table: str, audit_cols: list, start_date: dt.datetime) -> str:
        pass

    def get_row_count(self, table: str) -> int:
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

    def truncate(self, table: str) -> bool:
        pass

    def run_script(self, scriptfilepath: str) -> bool:
        pass

    def get_row_count(self, table: str) -> int:
        pass

    def get_latest_date_in_table(self, table: str, audit_cols: list =None) -> dt.datetime:
        pass

#Implementations of Sources and Targets
class Oracle(Source):
    pass


class Postgres(Source):
    pass


class Snowflake(Target):
    pass

