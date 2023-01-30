import os
from subprocess import call

import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv


def create_local_data_file(records: list, file_path: str, file_name: str, delimiter: str ="\t") -> str:
    try:
        create_local_panda_csv(records, file_path, file_name, delimiter)
    except OverflowError:
        create_local_csv(records, file_path, file_name, delimiter)

def create_local_panda_csv(records: list, file_path: str, file_name: str, delimiter: str ="\t") -> str:
    if not os.path.isdir(file_path):
        os.makedirs(file_path)
    full_path = os.path.join(file_path, file_name)
    
    data_df = pd.DataFrame(records)
    data_table = pa.Table.from_pandas(data_df)
    write_options = csv.WriteOptions(delimiter=delimiter, include_header=False)
    csv.write_csv(data_table, full_path, write_options=write_options)
    return full_path

def create_local_csv(records: list, file_path: str, file_name: str, delimiter: str ="\t") -> str:
    full_path = os.path.join(file_path, file_name)
    with open(full_path, "w+") as local_csv:
        csv_writer = csv.writer(    local_csv, 
                                    delimiter=delimiter,
                                    quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL
                                )
        for row in records:
            csv_writer.writerow(row)
    return full_path

def remove_files_in_local_folder(folder_path: str) -> bool:
    if os.path.isdir(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                    return True
            except Exception as err:
                print(err)
                return False
    else:
        return False
            