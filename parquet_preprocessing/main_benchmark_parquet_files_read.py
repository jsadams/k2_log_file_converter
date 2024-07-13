# import sys
# import os
# HOME = os.getenv('HOME')
#
# pylab_path=os.path.join(HOME,"usr","share","pylab")
# sys.path.append(pylab_path)
import copy

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

import os




#filename="selma_control_2023_0726_1022_17.dat"

#label='Hold time cylec #1 no windows, short tails, no fpa'

#filename="selma_control_2023_1228_0817_22.dat"

#filename='patty_control_2022_0901_1543_22.dat'
##filename='patty_control_2022_0907_1606_51.dat'
#filename='patty_control_2022_0914_1630_12.dat'

#filename='patty_control_2022_0915_1652_25.dat'

run_label='run_07_flex_hpd_heatsink'
csv_file_name='selma_control_2024_0117_1104_50.dat'


#run_label='run_09'
#filename='selma_control_2024_0220_1208_57.dat'

#filename='patty_control_2022_0919_0931_17.dat'

verbose=True

data_dir=data_dir_utils.get_data_dir()
df_csv=k2_control_log_reader_writer.read_k2_control_file_as_csv(csv_file_name,data_dir)

k2_control_log_reader_writer.pretty_print_df_read_stats(df_csv,df_csv)


# 'filename': 'selma_control_2024_0117_1104_50.dat',
# 'data_dir': '/Users/jsadams/Documents/k2-control-selma',
# 'file_size_bytes': 176984715,
# 'elapsed_time': 18.674908250002773,
# 'read_rate_bps': 9477139.733737312}

parquet_filename= k2_control_log_reader_writer.replace_filename_extension(csv_file_name, new_extension=".parquet")

compressions=['none', 'snappy', 'gzip', 'brotli', 'lz4', 'zstd']

list_of_dicts=list()
for i, compression in enumerate(compressions):


    append_string="_" + compression
    parquet_file_name_modified_i=k2_control_log_reader_writer.append_string_before_extension(parquet_filename, append_string)

    df_i=k2_control_log_reader_writer.read_k2_control_file_as_parquet(parquet_file_name_modified_i, data_dir)

    k2_control_log_reader_writer.pretty_print_df_read_stats(df_i,df_csv)

    record_i=copy.deepcopy(df_i.attrs)


list_of_dicts.append(record_i)


df_timings=pd.DataFrame(list_of_dicts)
