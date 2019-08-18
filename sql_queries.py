# DROP TABLES QUERIES
songplay_table_drop = "DROP table songplay"
user_table_drop = "DROP table user"
song_table_drop = "DROP table song"
artist_table_drop = "DROP table artist"
time_table_drop = "DROP table time"


# CREATE TABLES QUERIES
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (songplay_id varchar, \
                                                                 start_time bigint NOT NULL, \
                                                                 user_id varchar NOT NULL, \
                                                                 level varchar, \
                                                                 song_id varchar NOT NULL, \
                                                                 artist_id varchar NOT NULL, \
                                                                 session_id int, \
                                                                 location varchar, \
                                                                 user_agent varchar,
                                                                 PRIMARY KEY (songplay_id))""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS "user" (user_id varchar, \
                                                           first_name varchar, \
                                                           last_name varchar, \
                                                           gender varchar, \
                                                           level varchar,
                                                           PRIMARY KEY (user_id))""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (song_id varchar, \
                                                         title varchar, \
                                                         artist_id varchar, \
                                                         year int, \
                                                         duration float, \
                                                         PRIMARY KEY (song_id))""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (artist_id varchar, \
                                                             name varchar NOT NULL, \
                                                             location varchar, \
                                                             latitude varchar, \
                                                             longitude varchar,
                                                             PRIMARY KEY (artist_id))""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time bigint, \
                                                         hour int NOT NULL, \
                                                         day int NOT NULL, \
                                                         week int NOT NULL, \
                                                         month int NOT NULL, \
                                                         year int NOT NULL, \
                                                         weekday int NOT NULL,
                                                         PRIMARY KEY (start_time))""")


# INSERT RECORDS QUERIES
songplay_table_insert = ("""INSERT INTO songplay (songplay_id, start_time, user_id, level, song_id, \
                                                  artist_id, session_id, location, user_agent) \
                                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO "user" (user_id, first_name, last_name, gender, level) \
                                          VALUES (%s, %s, %s, %s, %s) \
                                          ON CONFLICT (user_id) DO UPDATE \
                                          SET level = excluded.level""")

song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration) \
                                          VALUES (%s, %s, %s, %s, %s) \
                                          ON CONFLICT (song_id) DO NOTHING""")

artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude) \
                                              VALUES (%s, %s, %s, %s, %s) \
                                              ON CONFLICT (artist_id) DO NOTHING""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                                          VALUES (%s, %s, %s, %s, %s, %s, %s) \
                                          ON CONFLICT (start_time) DO NOTHING""")


# FIND SONGS
song_select = ("""select song.song_id, song.artist_id, artist.name, song.title, song.duration from song join artist on song.artist_id = artist.artist_id""")


# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]