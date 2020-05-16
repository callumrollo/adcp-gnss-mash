from pathlib import Path
import pandas as pd
import datetime
import pynmea2
import numpy as np
import shutil
import zipfile

def car_time_to_iso(times_in):
    # transforms timestamps from Caravela GPS files into datetime objects and iso format strings
    times_out = []
    datetimes_out = []
    for time_point in times_in:
        dt_point = datetime.datetime.strptime(time_point, '%Y/%m/%d %H:%M:%S.%f')
        times_out.append(dt_point.isoformat()[:-4] + '+00:00')
        datetimes_out.append(dt_point)
    return times_out, datetimes_out

def nmea_msg_switch(gprmc_str):
    # takes gpprmc strings and returns gpgga and gpvtg for Nortek
    comps = gprmc_str.split(',')
    # Padding with dummy data to try and make Nortek software happy
    gpgga_str = str(pynmea2.GGA('GP', 'GGA', (comps[1], comps[3], comps[4], comps[5], comps[6],'2','09','0.9','5.3','M','51.9','M','2.6','0120')))
    # assuming that Nortek wants heading from course over ground (not heading of bow from true North) as it's all I got
    hehedt_str = str(pynmea2.HDG('HE', 'HDT', (comps[8], 'T')))
    # Massive assumptions: true bearing = mag bearing, caravela course over ground = heading
    gpvtg_str = str(pynmea2.VTG('GP', 'VTG', (comps[8], 'T', comps[8], 'M',comps[7],'N', str(float(comps[7])*1.94384), 'K', 'D')))
    return gpgga_str, gpvtg_str

def nmea_df_maker(gprmc_df):
    # make a dataframe of converted NMEA strings and nortek timestamps
    nmea_strs, timestamps = [], []
    for i in gprmc_df.index:
        gprmc_str, timestamp, _ = gprmc_df.iloc[i]
        gpgga_str, hehedt_str = nmea_msg_switch(gprmc_str)
        nmea_strs.append(gpgga_str)
        nmea_strs.append(hehedt_str)
        timestamps.append(timestamp)
        timestamps.append(timestamp)
    nmea_df = pd.DataFrame({'NMEAs': nmea_strs, 'timestamps': timestamps}, index=None)
    return nmea_df


# set the root to your data folder here
data_root = Path('/media/callum/storage/Documents/foo/adcp-car-prac-data/')

# find the SigVM files and extract their start times
adcp_files = list((data_root / 'adcp').rglob('*.SigVM'))
adcp_files.sort()

def path_and_times(adcp_file_list):
    df_paths = pd.DataFrame({'filepath': adcp_files})
    adcp_folder_paths = []
    adcp_file_out_paths = []
    # extract the start times of each file, end time defined as start time of next file or start + 1 day for final file
    starts, ends = [], []
    for i, filepath in enumerate(df_paths.filepath):
        path_str = str(filepath)
        file_out = list(filepath.parts)
        file_out[-2] = 'adcp_out'
        file_out[-1] = file_out[-1][:-6]
        adcp_file_out_paths.append(Path(*tuple(file_out)))
        adcp_folder_paths.append(Path(path_str[:-6]))
        starts.append(datetime.datetime(int(path_str[-24:-20]), int(path_str[-20:-18]), int(path_str[-18:-16]),
                                  int(path_str[-15:-13]), int(path_str[-13:-11]), int(path_str[-11:-9])))
        if i==len(df_paths)-1:
            ends.append(starts[-1] + datetime.timedelta(days=1))
        else:
            path_str = str(df_paths.filepath[i + 1])
            ends.append(datetime.datetime(int(path_str[-24:-20]), int(path_str[-20:-18]), int(path_str[-18:-16]),
                                  int(path_str[-15:-13]), int(path_str[-13:-11]), int(path_str[-11:-9])))
    df_paths['start'] = starts
    df_paths['end'] = ends
    df_paths['folder_paths'] = adcp_folder_paths
    df_paths['file_out_paths'] = adcp_file_out_paths
    return df_paths

df_paths = path_and_times(adcp_files)

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
     df_add = pd.DataFrame({'NMEA':df.NMEA, 'timestamp_nortek':df.time_utc, 'datetime':df.datetime}, index=None)
     df_gnss = df_gnss.append(df_add, sort=False)

df_gnss.index = np.arange(len(df_gnss))


# Extract the SigVM files

for file_zip, folder_path in zip(df_paths.filepath, df_paths.folder_paths):
    with zipfile.ZipFile(file_zip, 'r') as zip_ref:
        zip_ref.extractall(folder_path)

def make_gps_files(df_gnss):
    # for each adcp file, extract all gnss data from the relevant time period, convert NMEA strings and make .gps file
    for i in np.arange(len(df_paths)):
        df_sub = df_gnss[df_gnss.datetime>df_paths.start[i]][df_gnss.datetime<df_paths.end[i]]
        df_sub.index = np.arange(len(df_sub))
        nmea_new_format = nmea_df_maker(df_sub)
        gps_file_path = df_paths.folder_paths[i] / (str(df_paths.folder_paths[i].parts[-1])+'.gps')
        nmea_new_format.to_csv(gps_file_path, sep='#', header=False, index=False, line_terminator='\r\n')

make_gps_files(df_gnss)

# Make zip archives including the gps file
for file_out, folder_in in zip(df_paths.file_out_paths, df_paths.folder_paths):
    shutil.make_archive(file_out, 'zip', folder_in)
    
# Almost done! This makes zip files though. 
# Rename to SigVM with a bash one liner:
# for f in *.zip; do mv -- "$f" "${f%.zip}.SigVM"; done