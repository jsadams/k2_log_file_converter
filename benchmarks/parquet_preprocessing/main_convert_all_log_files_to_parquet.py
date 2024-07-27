import sys
import pylab_utils
pylab_utils.add_pylab_to_syspath()

import pandas as pd
import data_dir_utils
import k2_control_log_reader_writer
import os
import glob



def list_files_with_extension(directory,ext=".dat"):

    str="*" + ext
    search_pattern = os.path.join(directory, str)

    #search_pattern = os.path.join(directory, '*.foo')

    # Use glob to find all files matching the pattern
    foo_files = glob.glob(search_pattern)

    return foo_files

data_dir_raw=data_dir_utils.get_data_dir_raw()
data_dir_processed=data_dir_utils.get_data_dir_processed()

dat_file_names=list_files_with_extension(data_dir_raw,ext='dat')

#compressions_for_parquet_pandas=['gzip', 'brotli', 'lz4', 'zstd']

compression='gzip'

list_of_dicts=list()
for dat_file_name in dat_file_names:

    try:



         parquet_filename=k2_control_log_reader_writer.replace_filename_extension(dat_file_name, new_extension=".parquet")
         df=k2_control_log_reader_writer.read_k2_control_file_as_csv(dat_file_name,data_dir_raw,convert_time_stamp=True,verbose=False)

         csv_stats_i=df.attrs["csv_stats"]
         parquet_stats=k2_control_log_reader_writer.write_k2_control_file_as_parquet(df, parquet_filename, data_dir_processed, compression=compression,verbose=True)

         full_filename_csv=os.path.join(data_dir_raw, dat_file_name)
         full_filename_parquet=os.path.join(data_dir_processed, parquet_filename)
         file_size_csv=k2_control_log_reader_writer.get_file_size_bytes(full_filename_csv)
         file_size_parquet=k2_control_log_reader_writer.get_file_size_bytes(full_filename_parquet)
         elapsed_time_csv=csv_stats_i["elapsed_time"]
         elapsed_time_parquet=parquet_stats["elapsed_time"]

         record_i={"filename": dat_file_name,
                   "read_time":elapsed_time_csv,
                   "write_time": elapsed_time_parquet,
                   "file_size_csv":file_size_csv,
                   "file_size_parquet":file_size_parquet,
                   "ratio":file_size_parquet/file_size_csv}

         print(f"{dat_file_name}",end="")
         print(f"\t read_time {elapsed_time_csv:.2f} s",end="")
         print(f"\t write_time {elapsed_time_parquet:.2f} s",end="")

         print(f"\t file size csv  {file_size_csv/1e6:.2f} Mb", end="")
         print(f"\t file size parquet  {file_size_parquet/1e6:.2f} Mb", end="")
         print(f"\t ratio  {file_size_parquet/file_size_csv:.2%}", end="")

         print(f"")
         sys.stdout.flush()



    except Exception as e:
         print(e)
         #raise(e)

list_of_dicts.append(record_i)



df_timings=pd.DataFrame(list_of_dicts)


print(f"_________________________________________________________________")
df_timings.sort_values(by=['file_size'], ascending=True,inplace=True)


for index, record_i in df_timings.iterrows():


    filename=record_i['filename']

    file_size=record_i['file_size']
    elapsed_time=record_i['elapsed_time']
    print(f"{filename} \t written in {elapsed_time:.2f} seconds",end="")
    print(f"\t file size {file_size/1e6:.2f} Mb", end="")

    print(f"")

