from pathlib import Path
import pandas as pd
import datetime
import pynmea2
import numpy as np
import shutil
import zipfile

# set the root to your data folder here. After that just run the file
# expected folders under the data_root folder are:
# <data_root>/adcp for adcp files
# <data_root>/pcs Nortek PCS files
# <data_root>/adcp_out empty folder for the final files
data_root = Path('/media/callum/storage/Documents/foo/adcp-car-prac-data/')


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
        gprmc_str, timestamp, _ = gprmc_df.iloc[i]
        if gprmc_str[1:6] == 'GPGGA':
            nmea_strs.append(gprmc_str)
            timestamps.append(timestamp)

        elif gprmc_str[1:6] == 'PCHPR':
            nmea_strs.append(pynmea2.HDT('HE', 'HDT', (gprmc_str.split(',')[1], 'T')))
            timestamps.append(timestamp)
    nmea_df = pd.DataFrame({'NMEAs': nmea_strs, 'timestamps': timestamps}, index=None)
    return nmea_df




# find the SigVM files and extract their start times
adcp_files = list((data_root / 'adcp').rglob('*.SigVM'))
adcp_files.sort()


def path_and_times():
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
        adcp_folder_paths.append(Path(path_str[:-6]))
        starts.append(datetime.datetime(int(path_str[-24:-20]), int(path_str[-20:-18]), int(path_str[-18:-16]),
                                        int(path_str[-15:-13]), int(path_str[-13:-11]), int(path_str[-11:-9])))
        if i == len(df_path_and_times) - 1:
            ends.append(starts[-1] + datetime.timedelta(days=1))
        else:
            path_str_end = str(df_path_and_times.filepath[i + 1])
            ends.append(datetime.datetime(int(path_str_end[-24:-20]), int(path_str_end[-20:-18]), int(path_str_end[-18:-16]),
                                          int(path_str_end[-15:-13]), int(path_str_end[-13:-11]), int(path_str_end[-11:-9])))
    df_path_and_times['start'] = starts
    df_path_and_times['end'] = ends
    df_path_and_times['folder_paths'] = adcp_folder_paths
    df_path_and_times['file_out_paths'] = adcp_file_out_paths
    return df_path_and_times


df_paths = path_and_times()

# open all the GNSS files and make a table of the location messages
gnss_files = list((data_root / 'loc').rglob('*.dat'))
gnss_files.sort()

df_gnss = pd.DataFrame({})

# Go through the location files in order
for file in gnss_files:
    # read in the location csvs as raw strings
    df = pd.read_csv(file, sep=';', names=['raw_string'])
    # Look for the GPRMC strings
    df['gprmc_ident'] = df.raw_string.str[33:39]
    # drop the lines that aren't $GPRMC strings
    df = df[df['gprmc_ident'] == '$GPRMC']
    df.drop('gprmc_ident', 1, inplace=True)
    # extract the time stamps, NMEA strings and checksums
    df['time_stamp'] = df.raw_string.str[:23]
    # Asssuming UTC for caravela timestamps
    df['time_utc'], df['datetime'] = car_time_to_iso(df.time_stamp)
    df.drop('time_stamp', 1, inplace=True)
    df['NMEA'] = df.raw_string.str[33:]
    # Take just the NMEA string datetime and Nortek formatted timestamp
    df_add = pd.DataFrame({'NMEA': df.NMEA, 'timestamp_nortek': df.time_utc, 'datetime': df.datetime}, index=None)
    df_gnss = df_gnss.append(df_add, sort=False)

df_gnss.index = np.arange(len(df_gnss))

pcs_files = list((data_root / 'pcs').rglob('*_D_*'))
df_pcs = pd.DataFrame({})
for file in pcs_files:
    # read in the location csvs as raw strings
    df = pd.read_csv(file, sep=';', names=['raw_string'])
    # Look for the GPGGA and PCHPR strings
    df['nmea_ident'] = df.raw_string.str[16:22]
    df['desired'] = False
    df.loc[df['nmea_ident'] == '$GPGGA', 'desired'] = True
    df.loc[df['nmea_ident'] == '$PCHPR', 'desired'] = True
    df = df[df.desired]
    df['time_stamp'] = df.raw_string.str[:15]
    df['NMEA'] = df.raw_string.str[16:]
    df_uniq = df.drop_duplicates(subset=['nmea_ident', 'time_stamp'], keep='first')
    df_uniq['time_utc'], df_uniq['datetime'] = car_time_to_iso(df_uniq.time_stamp)
    df_add = pd.DataFrame({'NMEA': df_uniq.NMEA, 'timestamp_nortek': df_uniq.time_utc, 'datetime': df_uniq.datetime},
                          index=None)
    df_pcs = df_pcs.append(df_add, sort=False)
df_pcs.drop_duplicates(subset=['NMEA', 'datetime'], keep=False)
df_pcs.index = np.arange(len(df_pcs))

# Extract the SigVM files
for file_zip, folder_path in zip(df_paths.filepath, df_paths.folder_paths):
    with zipfile.ZipFile(file_zip, 'r') as zip_ref:
        zip_ref.extractall(folder_path)


def make_gps_files(df_nmea_msgs):
    # for each adcp file, extract all gnss data from the relevant time period, convert NMEA strings and make .gps file
    for i in np.arange(len(df_paths)):
        df_sub = df_nmea_msgs[df_nmea_msgs.datetime > df_paths.start[i]][df_nmea_msgs.datetime < df_paths.end[i]]
        df_sub.index = np.arange(len(df_sub))
        nmea_new_format = nmea_df_maker(df_sub)
        gps_file_path = df_paths.folder_paths[i] / (str(df_paths.folder_paths[i].parts[-1]) + '.gps')
        nmea_new_format.to_csv(gps_file_path, sep='#', header=False, index=False, line_terminator='\r\n')


make_gps_files(df_pcs)

# Make zip archives including the gps file
for file_out, folder_in in zip(df_paths.file_out_paths, df_paths.folder_paths):
    shutil.make_archive(file_out, 'zip', folder_in)

# Almost done! This makes zip files though. You'll need to rename to .SigVM
# Here's a bash one liner for the job. If you're on Windows you'll have to figure something else
# for f in *.zip; do mv -- "$f" "${f%.zip}.SigVM"; done
