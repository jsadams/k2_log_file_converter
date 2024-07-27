import numpy as np
from astropy import units as u

import filter_utils
import k2_control_log_utils
import plot_cycle_data
import k2_control_log_reader_writer


#import scipy

def process_file(data_dir, file_i):
     print(f"Processing {file_i}")
     df_full=read_k2_control_file.read_k2_control_file(file_i,data_dir)




     t_h=df_full["uptime_h"]
     t_s=df_full["uptime_s"]
     
     T_adr=df_full['T_adr']
     T_target=100e-3
     dT_target=5e-3
    

     keep=filter_utils.get_window_where_temperature_is_constant(t_s, T_adr, T_target, dT_target,time_window_over_which_to_average_s=60,graff=False)




     df_base_temp=df_full[keep]
     #results_dict=k2_control_log_utils.pretty_print_base_temp_statistics(df_base_temp)
     summary_dict=k2_control_log_utils.generate_base_temp_statistics(df_base_temp)



     plot_cycle_data.plot_cycle_data(df_full,keep)
     return summary_dict


def generate_base_temp_statistics(df_base_temp):
    """


    """

    use_units=False
    
    df_mean=df_base_temp.mean()
    df_max=df_base_temp.max()
    df_min=df_base_temp.min()
    
    preferred_unit_dict={'T_adr':u.mK,
                         'T_300mK':u.mK,
                         'T_3K':u.K,
                         'T_50K':u.K,
                         'SP_err_smoothed':u.nK}
    
    
    #keys_mean_mK=['T_adr', 'T_300mK']

    results_dict={}
    results_dict["timestamp_start"]=df_base_temp["timestamp"].min()
    results_dict['filename']=df_base_temp.attrs["filename"]
    
    keys_mean=['T_adr', 'T_300mK', 'T_3K', 'T_50K', 'T_adr']    
    for i, key_i in enumerate(keys_mean):
        x=df_mean[key_i]
        
        if use_units: 
             if key_i in preferred_unit_dict:
                  unit_preferred=preferred_unit_dict[key_i]
                  unit_si=(1*unit_preferred).si.unit
                  
                  x=x*unit_si
                  x=x.to(unit_preferred)

        #print(f"{key_i}_mean = \t{x:0.2f}")
        results_dict[key_i]=x
        
    keys_max=['I_mag']
    for i, key_i in enumerate(keys_max):
        #print(f"{key_i}_max = \t {df_max[key_i]}")
        results_dict[key_i]=x

    holdtime_s=(df_max["uptime_s"]-df_min["uptime_s"])*u.s
    holdtime_h=holdtime_s.to(u.h).value


    if np.isnan(holdtime_h):
          holdtime_h=None

    # if np.isnan(holdtime):
    #      holdtime=-999*u.h
    # else:         
    #      holdtime=holdtime.to(u.h)

    results_dict["holdtime_h"]=holdtime_h
    
    
    #print(f"holdtime = \t {holdtime:0.2f}")


    #results_dict["timestamp_end"]=df_base_temp["timestamp"].max()

    return results_dict


def plot_sp_err_smoothed(df_base_temp):
    import matplotlib.pyplot as plt

    SP_err_nK=df_base_temp['SP_err_smoothed']*1e9

    d=SP_err_nK
    hist, bin_edges = np.histogram(d,bins=100)

            
    plt.clf()
    fig = plt.figure(1)

    ### subplot nrows, ncols, plot #
    ax1 = fig.add_subplot(111)
        
    if False:
        # An "interface" to matplotlib.axes.Axes.hist() method
        n, bins, patches = plt.hist(x=d, bins='auto', color='#0504aa', alpha=0.7, rwidth=0.85)
        plt.grid(axis='y', alpha=0.75)
        plt.xlabel('Value')
        plt.ylabel('Frequency')
        plt.title('My Very Own Histogram')
        plt.text(23, 45, r'$\mu=15, b=3$')
        maxfreq = n.max()
        # Set a clean upper y-axis limit.
        plt.ylim(ymax=np.ceil(maxfreq / 10) * 10 if maxfreq % 10 else maxfreq + 10)



    if True:
        d.plot.hist(grid=True, bins='auto')
        plt.title('Commute Times for 1,000 Commuters')
        plt.xlabel('Counts')
        plt.ylabel('Commute Time')
        plt.grid(axis='y', alpha=0.75)
