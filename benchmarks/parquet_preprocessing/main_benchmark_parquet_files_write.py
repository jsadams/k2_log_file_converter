# import sys
# import os
# HOME = os.getenv('HOME')
#
# pylab_path=os.path.join(HOME,"usr","share","pylab")
# sys.path.append(pylab_path)
import pylab_utils
pylab_utils.add_pylab_to_syspath()



import math
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import astropy
from astropy import units as u
from astropy.table import QTable, Table, Column
#import scipy 
from scipy import signal

import os

import k2_control_log_reader_writer
import plot_cycle_data
from csd2 import plot_utils
import filter_utils
import k2_control_log_utils
import data_dir_utils
import k2_control_log_reader_writer

import os
import polars as pl
import timeit

#filename="selma_control_2023_0726_1022_17.dat"

#label='Hold time cylec #1 no windows, short tails, no fpa'

#filename="selma_control_2023_1228_0817_22.dat"

#filename='patty_control_2022_0901_1543_22.dat'
##filename='patty_control_2022_0907_1606_51.dat'
#filename='patty_control_2022_0914_1630_12.dat'

#filename='patty_control_2022_0915_1652_25.dat'

run_label='run_07_flex_hpd_heatsink'
dat_file_name='selma_control_2024_0117_1104_50.dat'


#run_label='run_09'
#filename='selma_control_2024_0220_1208_57.dat'

#filename='patty_control_2022_0919_0931_17.dat'

data_dir_raw=data_dir_utils.get_data_dir_raw()
df_full=k2_control_log_reader_writer.read_k2_control_file_as_csv(dat_file_name,data_dir_raw,convert_time_stamp=False)


data_dir=data_dir_utils.get_data_dir_processed()


#df_pl=pl.from_pandas(df_full)


#start_time = timeit.default_timer()


# compression=""
# hdf_filename=k2_control_log_reader_writer.replace_filename_extension(csv_file_name, new_extension=".h5")
# file_path =os.path.join(data_dir, hdf_filename)
# df_pl.write_avro(file_path,key='df_full')
#
# df = pl.from_pandas(pd_df)
# elapsed_time = timeit.default_timer() - start_time
# file_size_bytes=get_file_size_bytes(file_path)
# file_size_Mb=get_file_size_Mb(file_path)
# if verbose:
#     print(f"{filename} \t written in {elapsed_time:.2f} seconds",end="")
#     print(f"\t file size {file_size_Mb:.2f} Mb", end="")
#     print(f"\t write rate {file_size_Mb/elapsed_time:.2f} Mbps",end="")
#     print(f"")
#
# record_i={"compression":compression, "filename":filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}




list_of_dicts=list()

#feather_filename=k2_control_log_reader_writer.replace_filename_extension(dat_file_name, new_extension=".feather")
#df_full.to_feather(feather_filename)

csv_filename=k2_control_log_reader_writer.replace_filename_extension(dat_file_name, new_extension=".csv")
record_i=k2_control_log_reader_writer.write_k2_control_file_as_csv(df_full, csv_filename, data_dir)
list_of_dicts.append(record_i)


json_filename=k2_control_log_reader_writer.replace_filename_extension(dat_file_name, new_extension=".json")
record_i=k2_control_log_reader_writer.write_k2_control_file_as_json(df_full, json_filename, data_dir)
list_of_dicts.append(record_i)


pickle_filename=k2_control_log_reader_writer.replace_filename_extension(dat_file_name, new_extension=".pkl")
record_i=k2_control_log_reader_writer.write_k2_control_file_as_pickle(df_full, pickle_filename, data_dir)
list_of_dicts.append(record_i)

##########################################################################
#
#
#  avro
#
#
##########################################################################

compressions_for_avro_polars=['uncompressed', 'snappy', 'deflate']
avro_filename=k2_control_log_reader_writer.replace_filename_extension(dat_file_name, new_extension=".avro")

for i, compression in enumerate(compressions_for_avro_polars):


    append_string="_" + compression
    parquet_file_name_modified_i=k2_control_log_reader_writer.append_string_before_extension(avro_filename, append_string)

    record_i=k2_control_log_reader_writer.write_k2_control_file_as_avro(df_full, parquet_file_name_modified_i, data_dir, compression=compression)

    list_of_dicts.append(record_i)

##########################################################################
#
#
#  parquet
#
#
##########################################################################

compressions_for_parquet_pandas=['none', 'snappy', 'gzip', 'brotli', 'lz4', 'zstd']
parquet_filename=k2_control_log_reader_writer.replace_filename_extension(dat_file_name, new_extension=".parquet")

for i, compression in enumerate(compressions_for_parquet_pandas):


    append_string="_" + compression
    parquet_file_name_modified_i=k2_control_log_reader_writer.append_string_before_extension(parquet_filename, append_string)

    record_i=k2_control_log_reader_writer.write_k2_control_file_as_parquet(df_full, parquet_file_name_modified_i, data_dir, compression=compression)

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

