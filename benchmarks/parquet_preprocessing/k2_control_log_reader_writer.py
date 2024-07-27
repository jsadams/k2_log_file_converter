import os
import timeit
import sys
import pandas as pd
import polars as pl

def pretty_print_df_read_stats(df_i,df_csv):

    filename=df_i.attrs['filename']
    data_dir=df_i.attrs['data_dir']
    file_size_bytes=df_i.attrs['file_size_bytes']
    elapsed_time=df_i.attrs['elapsed_time']
    read_rate_Mbps=df_i.attrs['read_rate_bps']/1e6
    file_size_Mb=file_size_bytes/1e6

    file_size_ratio=df_i.attrs['file_size_bytes']/df_csv.attrs['file_size_bytes']
    speed_ratio=df_i.attrs['elapsed_time']/df_csv.attrs['elapsed_time']


    print(f"{filename} \t read in {elapsed_time:.2f} seconds",end="")
    print(f"\t file size {file_size_Mb:.2f} Mb", end="")
    print(f"\t read rate {read_rate_Mbps:.2f} Mbps",end="")
    print(f"\t file_size_ratio {file_size_ratio:.2%}",end="")
    print(f"\t speed_ratio {speed_ratio:.2%}",end="")

    print(f"")



def get_file_size_bytes(filename):
    return os.stat(filename).st_size

def get_file_size_Mb(filename):
    return os.stat(filename).st_size/(1024*1024)

def replace_filename_extension(csv_file_path, new_extension=".parquet"):
    # Create output file path
    base_name = os.path.basename(csv_file_path)
    output_file_name = os.path.splitext(base_name)[0] + new_extension
    #output_file_path = os.path.join(output_dir, output_file_name)

    return output_file_name

def append_string_before_extension(filename, string_to_append):
    """

        converts data.dat --> data_foo.dat

    :param filename:
    :param new_string:
    :return:
    """
    basename, file_extension = os.path.splitext(filename)
    new_filename = basename+string_to_append+file_extension


    return new_filename


def read_k2_control_file_as_parquet(parquet_file_name, data_dir):
    full_filename = os.path.join(data_dir, parquet_file_name)

    start_time = timeit.default_timer()
    df = pd.read_parquet(full_filename)
    elapsed_time = timeit.default_timer() - start_time

    df.attrs["filename"]=parquet_file_name
    df.attrs["data_dir"]=data_dir
    df.attrs["file_size_bytes"]=get_file_size_bytes(full_filename)
    df.attrs["elapsed_time"]=elapsed_time
    df.attrs["read_rate_bps"]=df.attrs["file_size_bytes"]/df.attrs["elapsed_time"]

    return df

def read_k2_control_file_as_csv(csv_filename, data_dir, convert_time_stamp=True,verbose=True):

    full_filename = os.path.join(data_dir, csv_filename)

    #file_size_Mb=get_file_size_Mb(full_filename)

    #ddf = pd.read_csv(full_filename, delim_whitespace=True)
    #df = pd.read_csv(full_filename,sep='\s+')

    if verbose:
        print(f"Reading {full_filename}")
        sys.stdout.flush()

    start_time = timeit.default_timer()
    df = pd.read_csv(full_filename,sep="\\s+")
    elapsed_time = timeit.default_timer() - start_time

    if convert_time_stamp:
      try:
        tv_sec = df['t_tv_sec']
        pd.Timestamp(tv_sec[0], unit='s', tz='US/Eastern')
        # for x_i in x:
        #     print(f"x={x_i}")
        the_timestamps = [pd.Timestamp(tv_sec_i, unit='s', tz='US/Eastern') for tv_sec_i in tv_sec]

        #df = df.drop(["t_day", "t_year", "t_mon", "t_hr", "t_min", "t_sec", "t_tv_usec", "t_tv_sec"], axis=1)
        df = df.drop(["t_day", "t_year", "t_mon", "t_hr", "t_min", "t_sec"], axis=1)


        # df.insert(0, 'TimeStamp', pd.to_datetime('now').replace(microsecond=0)
        df = df.rename(columns={"t_s": "uptime_s", "t_m": "uptime_min", "t_h": "uptime_h"})
        df = df.drop(["uptime_min", "uptime_h"], axis=1)



        df["timestamp"] = the_timestamps
        t0 = df['timestamp'][0]
        dt = df['timestamp'] - t0
        df["timedelta"] = dt

        df['dt_hours'] = df['timedelta'] / pd.Timedelta(hours=1)
        df['dt_secs'] = df['timedelta']  / pd.Timedelta(seconds=1)
      except Exception as e:
        print(f"on {filename} expection {e}")
        #df=pd.DataFrame()

    df.attrs["filename"]=csv_filename
    df.attrs["data_dir"]=data_dir
    file_size_bytes=get_file_size_bytes(full_filename)
    #df.attrs["file_size_bytes"]=get_file_size_bytes(full_filename)
    #df.attrs["file_size_Mb"]=get_file_size_Mb(full_filename)
    #df.attrs["elapsed_time"]=elapsed_time
    #df.attrs["read_rate_bps"]=df.attrs["file_size_bytes"]/df.attrs["elapsed_time"]
    #df.attrs["file_size_Mb"]=get_file_size_Mb(full_filename)

    record_i={"format": "csv", "compression": "none", "filename":csv_filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}

    df.attrs["csv_stats"]=record_i
    return df


def write_k2_control_file_as_parquet(df, filename, data_dir, compression='snappy', verbose=False):

    full_filename =os.path.join(data_dir,filename)

    start_time = timeit.default_timer()
    df.to_parquet(full_filename,compression=compression)
    elapsed_time = timeit.default_timer() - start_time

    file_size_bytes=get_file_size_bytes(full_filename)
    file_size_Mb=get_file_size_Mb(full_filename)

    if verbose:
        print(f"{filename} \t written in {elapsed_time:.2f} seconds",end="")
        print(f"\t file size {file_size_Mb:.2f} Mb", end="")
        print(f"\t write rate {file_size_Mb/elapsed_time:.2f} Mbps",end="")
        print(f"")

    record_i={"format": "parquet", "compression":compression, "filename":filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}


    return record_i
def write_k2_control_file_as_avro(df, filename, data_dir, compression='snappy', verbose=False):

    ## we will use polars for this
    df_pl=pl.from_pandas(df)
    
    full_filename =os.path.join(data_dir,filename)

    start_time = timeit.default_timer()
    df_pl.write_avro(full_filename,compression=compression)
    #df.to_parquet(full_filename,compression=compression)
    elapsed_time = timeit.default_timer() - start_time

    file_size_bytes=get_file_size_bytes(full_filename)
    file_size_Mb=get_file_size_Mb(full_filename)

    if verbose:
        print(f"{filename} \t written in {elapsed_time:.2f} seconds",end="")
        print(f"\t file size {file_size_Mb:.2f} Mb", end="")
        print(f"\t write rate {file_size_Mb/elapsed_time:.2f} Mbps",end="")
        print(f"")

    #record_i={"compression":compression, "filename":filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}
    record_i={"format": "avro", "compression":compression, "filename":filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}


    return record_i

def write_k2_control_file_as_pickle(df, filename, data_dir, compression="", verbose=False):

    file_path =os.path.join(data_dir, filename)

    start_time = timeit.default_timer()
    df.to_pickle(file_path)
    elapsed_time = timeit.default_timer() - start_time

    file_size_bytes=get_file_size_bytes(file_path)
    file_size_Mb=get_file_size_Mb(file_path)

    if verbose:
        print(f"{filename} \t written in {elapsed_time:.2f} seconds",end="")
        print(f"\t file size {file_size_Mb:.2f} Mb", end="")
        print(f"\t write rate {file_size_Mb/elapsed_time:.2f} Mbps",end="")
        print(f"")

    #record_i={"compression":compression, "filename":filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}
    record_i={"format": "pickle", "compression":compression, "filename":filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}


    return record_i

def write_k2_control_file_as_json(df, filename, data_dir, compression="", verbose=False):

    file_path =os.path.join(data_dir, filename)

    start_time = timeit.default_timer()
    df.to_json(file_path)
    elapsed_time = timeit.default_timer() - start_time

    file_size_bytes=get_file_size_bytes(file_path)
    file_size_Mb=get_file_size_Mb(file_path)

    if verbose:
        print(f"{filename} \t written in {elapsed_time:.2f} seconds",end="")
        print(f"\t file size {file_size_Mb:.2f} Mb", end="")
        print(f"\t write rate {file_size_Mb/elapsed_time:.2f} Mbps",end="")
        print(f"")

    #record_i={"compression":compression, "filename":filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}
    record_i={"format": "json", "compression":compression, "filename":filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}


    return record_i

def write_k2_control_file_as_csv(df, filename, data_dir, compression="", verbose=False):

    file_path =os.path.join(data_dir, filename)

    start_time = timeit.default_timer()
    df.to_csv(file_path)
    elapsed_time = timeit.default_timer() - start_time

    file_size_bytes=get_file_size_bytes(file_path)
    file_size_Mb=get_file_size_Mb(file_path)

    if verbose:
        print(f"{filename} \t written in {elapsed_time:.2f} seconds",end="")
        print(f"\t file size {file_size_Mb:.2f} Mb", end="")
        print(f"\t write rate {file_size_Mb/elapsed_time:.2f} Mbps",end="")
        print(f"")

    #record_i={"compression":compression, "filename":filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}
    record_i={"format": "json", "compression":compression, "filename":filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}


    return record_i


# def write_parquet_for_all_compressions(df, filename, data_dir):
# 
# 
#     #compressions=["snappy","None","gzip"]
#     compressions=['snappy', 'gzip', 'brotli', 'lz4', 'zstd']
# 
# 
#     #time_log=dict()
#     list_of_dicts=list()
#     for i, compression in enumerate(compressions):
# 
#         # base_name = os.path.basename(filename)
#         # new_extension = ".parquet"
#         #
#         # parquet_file_name_modified_i = os.path.splitext(base_name)[0] + "_" + compression + new_extension
# 
#         parquet_file_name_modified_i=append_string_before_extension(parquet_file_name_modified_i, new_extension="_"+compression)
#         #    full_filename =os.path.join(data_dir,output_file_name_i)
# 
#         record_i=write_k2_control_file_as_parquet(df, parquet_file_name_modified_i, data_dir, compression=compression)
# 
#         # start_time = timeit.default_timer()
#         #
#         # df.to_parquet(full_filename,compression=compression)
#         #
#         # # code you want to evaluate
#         # elapsed_time = timeit.default_timer() - start_time
#         #
#         # file_size_Mb=get_file_size_Mb(full_filename)
#         #
#         # #time_log[output_file_name_i]=elapsed_time
#         # print(f"{output_file_name_i} written  in {elapsed_time} seconds",end="")
#         # print(f"\t {file_size_Mb=}", end="")
#         # print(f"\t {file_size_Mb/elapsed_time} Mbps",end="")
#         # print(f"")
#         #
#         # record_i={"compression":compression, "filename":output_file_name_i, "elapsed_time":elapsed_time, "file_size_Mb":file_size_Mb}
# 
#         list_of_dicts.append(record_i)
# 
# 
#     return pd.DataFrame(list_of_dicts)


# def write_k2_control_file_as_something(df, filename, data_dir, compression="", verbose=True):
# 
#     file_path =os.path.join(data_dir, filename)
# 
#     start_time = timeit.default_timer()
#     df.to_pickle(file_path)
#     elapsed_time = timeit.default_timer() - start_time
# 
#     file_size_bytes=get_file_size_bytes(file_path)
#     file_size_Mb=get_file_size_Mb(file_path)
# 
#     if verbose:
#         print(f"{filename} \t written in {elapsed_time:.2f} seconds",end="")
#         print(f"\t file size {file_size_Mb:.2f} Mb", end="")
#         print(f"\t write rate {file_size_Mb/elapsed_time:.2f} Mbps",end="")
#         print(f"")
# 
#     record_i={"compression":compression, "filename":filename, "elapsed_time":elapsed_time, "file_size":file_size_bytes}
# 
# 
#     return record_i
