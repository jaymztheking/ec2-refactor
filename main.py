import os
import yaml

from db import Postgres, Snowflake
from config import pgdict

PROJECT_ROOT = "C:\\Users\\jmedaugh\\Github\\ec2-refactor"

#Read Yaml file and iterate over table config dicts
# if __name__ == "__main__":
#     with open(os.path.join(PROJECT_ROOT, "table-configs.yaml")) as f:
#         conf = yaml.load_all(f, Loader=yaml.FullLoader)
#         ingestion_stats_lst = list()
#         for table in conf:
#            if table is not None:
#                 for tbl, tbl_conf in table.items():
#                     ingestion_stats = dict()
#                     print(tbl_conf)
#                     if tbl_conf['Run']:
#                         print('DONE')
            
if __name__ == "__main__":

    source = Postgres(**pgdict)
    print(source.get_row_count('testtable', 'testschema'))
    if source.connect():
        answer = source.get_row_count('testtable', 'testschema')
        print(answer)
        print(type(answer))
        source.disconnect()
    print('fin')
    


