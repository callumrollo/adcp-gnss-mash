from pathlib import Path
import pandas as pd
import datetime
import pynmea2
import numpy as np
import shutil
import os


# set the root to your data folder here. After that just run the file
# expected folders under the data_root folder are:
# <data_root>/adcp for Nortek adcp files
# <data_root>/pcs Autonaut PCS files
# <data_root>/adcp_out empty folder for the final files

def main():
    data_root = Path('/media/callum/storage/Documents/foo/adcp-caravela-all-proc-data/')
    # find the SigVM files and extract their start times
    adcp_files = list((data_root / 'adcp_sub').rglob('*.SigVM'))
    adcp_files.sort()
    df_paths = path_and_times(adcp_files)

    pcs_files = list((data_root / 'pcs_sub').rglob('*_D_*'))
    df_pcs = pd.DataFrame({})
    for file in pcs_files:
        # read in the location csvs as raw strings
        df = pd.read_csv(file, sep=';', names=['raw_string'])
        # Look for the GPGGA and PCHPR strings
        df['nmea_ident'] = df.raw_string.str[16:22]
        df['desired'] = False
        df.loc[df['nmea_ident'] == '$GPGGA', 'desired'] = True
        df.loc[df['nmea_ident'] == '$PCHPR', 'desired'] = True
        df.loc[df['nmea_ident'] == '$GPRMC', 'desired'] = True
        df = df[df.desired]
        df['time_stamp'] = df.raw_string.str[:15]
        df['NMEA'] = df.raw_string.str[16:]
        df_uniq = df.drop_duplicates(subset=['nmea_ident', 'time_stamp'], keep='first')
        df_uniq['time_utc'], df_uniq['datetime'] = car_time_to_iso(df_uniq.time_stamp)
        df_add = pd.DataFrame(
            {'NMEA': df_uniq.NMEA, 'timestamp_nortek': df_uniq.time_utc, 'datetime': df_uniq.datetime},
            index=None)
        df_pcs = df_pcs.append(df_add, sort=False)
    df_pcs.drop_duplicates(subset=['NMEA', 'datetime'], keep=False)
    df_pcs.index = np.arange(len(df_pcs))
    make_gps_files(df_pcs, df_paths)

    # Make zip archives including the gps file then rename to SigVM files
    for file_out, folder_in in zip(df_paths.file_out_paths, df_paths.folder_paths):
        shutil.make_archive(file_out, 'zip', folder_in)
        os.rename(str(file_out) + '.zip', str(file_out) + '.SigVM')


def car_time_to_iso(times_in):
    # transforms timestamps from Caravela GPS files into datetime objects and iso format strings
    times_out = []
    datetimes_out = []
    for time_point in times_in:
        dt_point = datetime.datetime.strptime(time_point, '%y%m%d,%H:%M:%S')
        times_out.append(dt_point.isoformat() + '+00:00')
        datetimes_out.append(dt_point)
    return times_out, datetimes_out


def nmea_df_maker(gprmc_df):
    # make a dataframe of converted NMEA strings and nortek timestamps
    nmea_strs, timestamps = [], []
    for i in gprmc_df.index:
        nmea_str, timestamp, _ = gprmc_df.iloc[i]
        if nmea_str[1:6] == 'GPGGA':
            nmea_strs.append(nmea_str)
            timestamps.append(timestamp)
        elif nmea_str[1:6] == 'GPRMC':
            try:
                pynmea2.parse(nmea_str)
                comps = nmea_str.split(',')
                gpvtg_str = str(pynmea2.VTG('GP', 'VTG', (
                    comps[8], 'T', comps[8], 'M', comps[7], 'N', str(float(comps[7]) * 1.94384), 'K', 'D')))
                nmea_strs.append(gpvtg_str)
                timestamps.append(timestamp)
            except:
               print("bad gprmc string")
        elif nmea_str[1:6] == 'PCHPR':
            nmea_strs.append(pynmea2.HDT('HE', 'HDT', (nmea_str.split(',')[1], 'T')))
            timestamps.append(timestamp)
    nmea_df = pd.DataFrame({'NMEAs': nmea_strs, 'timestamps': timestamps}, index=None)
    return nmea_df


def path_and_times(adcp_files):
    df_path_and_times = pd.DataFrame({'filepath': adcp_files})
    adcp_folder_paths = []
    adcp_file_out_paths = []
    # extract the start times of each file, end time defined as start time of next file or start + 1 day for final file
    starts, ends = [], []
    for i, filepath in enumerate(df_path_and_times.filepath):
        path_str = str(filepath)
        file_parts = list(filepath.parts)
        file_parts[-2] = 'adcp_out'
        file_parts[-1] = file_parts[-1][:-6]
        adcp_file_out_paths.append(Path(*tuple(file_parts)))
        adcp_folder_paths.append(Path(path_str + '_FILES'))
        starts.append(datetime.datetime(int(path_str[-24:-20]), int(path_str[-20:-18]), int(path_str[-18:-16]),
                                        int(path_str[-15:-13]), int(path_str[-13:-11]), int(path_str[-11:-9])))
        if i == len(df_path_and_times) - 1:
            ends.append(starts[-1] + datetime.timedelta(days=1))
        else:
            path_str_end = str(df_path_and_times.filepath[i + 1])
            ends.append(
                datetime.datetime(int(path_str_end[-24:-20]), int(path_str_end[-20:-18]), int(path_str_end[-18:-16]),
                                  int(path_str_end[-15:-13]), int(path_str_end[-13:-11]), int(path_str_end[-11:-9])))
    df_path_and_times['start'] = starts
    df_path_and_times['end'] = ends
    df_path_and_times['folder_paths'] = adcp_folder_paths
    df_path_and_times['file_out_paths'] = adcp_file_out_paths
    return df_path_and_times


def make_gps_files(df_nmea_msgs, df_paths):
    # for each adcp file, extract all gnss data from the relevant time period, convert NMEA strings and make .gps file
    for i in np.arange(len(df_paths)):
        df_sub = df_nmea_msgs[df_nmea_msgs.datetime > df_paths.start[i]][df_nmea_msgs.datetime < df_paths.end[i]]
        df_sub.index = np.arange(len(df_sub))
        nmea_new_format = nmea_df_maker(df_sub)
        gps_file_path = df_paths.folder_paths[i] / (str(df_paths.folder_paths[i].parts[-1]) + '.gps')
        nmea_new_format.to_csv(gps_file_path, sep='#', header=False, index=False, line_terminator='\r\n')


if __name__ == '__main__':
    main()
