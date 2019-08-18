import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


def get_files(filepath):
    """This function helps us to go through all files which are ended with '.json'
    ,get their path, and save these path in an list.
    """
    # define an empty list for store all file path
    all_files = []
    
    # get each file path and put into all_file list
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))             
    return all_files


def process_song_file(conn, cur, filepath):
    """This function helps us get song data in all song.json files and put them all
    in an overall dataframe. After that, it creates two dataframes(song_data and artist 
    dataframe) by retrieving needed columns from the overall dataframe. Then, this 
    function moves data in song_data dataframe and artist dataframe to song table and 
    artist table in postgres database. This function can be seperated into three parts. 
    The part1 is getting data and save them in an overall dataframe. The part2 is 
    inserting data into song table in postgres database. The part3 is inserting data 
    into artist table in artist table in postgres.
    """    
    # PART1: retrieve data from all song files and save them in an overall dataframe 
    song_files = get_files(filepath)
    song_file_str = ""
    
    for i in range(0, len(song_files)):
        if i == 0:
            with open(song_files[i]) as song_content:
                for line in song_content.readlines():
                    song_file_str = song_file_str + line
        else:
            with open(song_files[i]) as song_content:
                for line in song_content.readlines():
                    song_file_str = song_file_str + ',' + line

    song_file_df = pd.read_json(song_file_str, lines=True)
    
    # clean the overall dataframe because it has some duplications and incongruent values
    # such as np.nan and ''. We will replace np.nan and '' by 'None' 
    song_file_df = song_file_df.drop_duplicates(keep='first')
    song_file_df = song_file_df.fillna(value='None')
    song_file_df = song_file_df.replace('', 'None')
    
    
    # PART2: insert data into song table
    # retrieve data from the overall dataframe and make song_data dataframe
    song_data_df = song_file_df.loc[:,['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data_list = song_data_df.values.tolist()
    
    # instert data into song table
    for row in song_data_list:
        cur.execute(song_table_insert, row)
        conn.commit()
 

    # PART3: insert data into artist table
    # retrieve data from the overall dataframe and make artist_data dataframe
    artist_data_df = song_file_df.loc[:,['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude"']]
    artist_data_list = artist_data_df.replace(np.nan, 'None').values.tolist()

    # instert data into artist table
    for row in artist_data_list:
        cur.execute(artist_table_insert, row)
        conn.commit()
    

def process_log_file(conn, cur, filepath):
    """This function helps us get log data in all log.json files and put them all
    in an overall dataframe. After that, it creates three dataframes(time, user, 
    and songplay dataframe) by retrieving needed columns from the overall dataframe. 
    Then, this function moves data from time, user and songplay dataframe to tables 
    in postgres database. This function can be seperated into four parts. Part1 is 
    getting data and save them in an overall dataframe. The part2 is inserting data 
    into postgres time table. Part3 is inserting data into postgres user table. Part4
    is inserting data into postgres songplay table
    """
    # PART1: retrieve data from all song files and save them in an overall dataframe 
    log_files = get_files(filepath)    
    log_file_str = ""

    for i in range(0, len(log_files)):
        if i == 0:
            with open(log_files[i]) as log_content:
                for line in log_content.readlines():
                    log_file_str = log_file_str + line
        else:
            with open(log_files[i]) as log_content:
                for line in log_content.readlines():
                    log_file_str = log_file_str + '\n' + line

    log_file_str_replaced = log_file_str.replace('}\n{', '},{')
    log_file_df = pd.read_json(log_file_str_replaced, lines=True)       
    
    # clean the overall dataframe because it has some duplications and incongruent values
    # such as np.nan and ''. We will replace np.nan and '' by 'None' '
    log_file_df = log_file_df.drop_duplicates(keep='first')
    log_file_df = log_file_df.fillna(value='None')
    log_file_df = log_file_df.replace('', 'None')

    
    # PART2: insert data into time table
    # retrieve data from the overall dataframe and make song_data dataframe and filter by
    # NextSong action    
    filtered_log_file_df = log_file_df[log_file_df['page'] == 'NextSong']    
    
    # convert timestamp column to datetime
    exact_time = pd.to_datetime(filtered_log_file_df['ts'], unit='ms')
    
    # Extract the timestamp, hour, day, week of year, month, year, weekday from ts column
    timestamp = filtered_log_file_df['ts']
    hour = exact_time.dt.hour
    day = exact_time.dt.day
    weekofYear = exact_time.dt.weekofyear
    month = exact_time.dt.month
    year = exact_time.dt.year
    weekday = exact_time.dt.weekday
    
    # put timestamp, hour, day, week of year, month, year, and weekday into a dataframe
    time_data_df = pd.concat([timestamp, hour, day, weekofYear, month, year, weekday], axis=1)
    time_data_df.columns = ['timestamp', 'hour', 'day', 'weekofYear', 'month', 'year', 'weekday']
    
    # clean the overall dataframe because it has some duplications    
    time_data_df = time_data_df.drop_duplicates(keep='first')
    time_data_list = time_data_df.values.tolist()

    # insert data into time table
    for row in time_data_list:
        cur.execute(time_table_insert, row)
        conn.commit()

                
    # PART3: insert data into user table        
    # retrieve data from the overall dataframe and make user dataframe
    user_df = log_file_df.loc[:,['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates(keep='first')
    user_list = user_df.values.tolist()

    # insert user records
    for row in user_list:
        cur.execute(user_table_insert, row)
        conn.commit()

        
    # PART4: insert data into songplay table          
    # read two tables and get the data needed (song and artist) from postgres data
    song_join_artist_df = pd.read_sql(song_select,conn)

    # change the datatype of duration column and reset the song_join_artist_df index 
    # for following merge operation
    song_join_artist_df['duration'] = song_join_artist_df['duration'].astype('object') 
    song_join_artist_df.index = list(song_join_artist_df.index)
    
    #reset the song_join_artist_df index for following merge operation    
    song_join_artist_join_logfile_df = pd.merge(log_file_df, song_join_artist_df, \
                                                left_on=['artist', 'song', 'length'], \
                                                right_on=['name', 'title', 'duration'], \
                                                how='left')
    
    #retrieve the column that songplay table needs and make the format song table wants
    songplay_df = song_join_artist_join_logfile_df.loc[:,['ts', 'userId', 'level', \
                                                          'song_id', 'artist_id', 'sessionId', \
                                                          'location', 'userAgent']]
    songplay_df.index = range(1,len(songplay_df)+1)
    songplay_df = songplay_df.reset_index()
    songplay_df.columns = ['songplay_id', 'start_time', 'user_id', \
                           'level', 'song_id', 'artist_id', \
                           'session_id', 'location', 'user_agent']
    songplay_df = songplay_df.fillna(value='None')
    songplay_df = songplay_df.replace('', 'None')
        
    # insert songplay records
    songplay_list = songplay_df.values.tolist()

    for row in songplay_list:
        cur.execute(songplay_table_insert, row)
        conn.commit()

        
def main():
    # set up the connection with postgres
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # process song_file and log_file
    process_song_file(conn, cur, '/home/workspace/data/song_data')    
    process_log_file(conn, cur, '/home/workspace/data/log_data')
 
    # close the connection with postgres
    conn.close()


if __name__ == "__main__":
    main()