import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""\
CREATE TABLE IF NOT EXISTS staging_events (
artist varchar, \
auth varchar, \
firstName varchar, \
gender CHAR(1), \
itemInSession integer NOT NULL, \
lastName varchar, \
length double precision, \
level varchar, \
location varchar, \
method varchar, \
page varchar, \
registration double precision, \
sessionid integer NOT NULL, \
song varchar, \
status integer, \
ts bigint NOT NULL, \
userAgent varchar, \
userId integer, \
primary key(sessionid, itemInSession)\
);\
""")

staging_songs_table_create = ("""\
CREATE TABLE IF NOT EXISTS staging_songs (\
num_songs integer NOT NULL, \
artist_id varchar NOT NULL, \
artist_latitude double precision, \
artist_longitude double precision, \
artist_location varchar, \
artist_name varchar NOT NULL, \
song_id varchar NOT NULL, \
title varchar NOT NULL, \
duration double precision NOT NULL, \
year integer, \
primary key(song_id)\
);\
""")

songplay_table_create = ("""\
CREATE TABLE IF NOT EXISTS songplay (\
songplay_id bigint IDENTITY(0,1) NOT NULL, \
start_time timestamp NOT NULL, \
user_id integer NOT NULL, \
level varchar, \
song_id varchar NOT NULL, \
artist_id varchar NOT NULL, \
session_id integer, \
location varchar, \
user_agent varchar, \
primary key(songplay_id), \
foreign key(user_id) references users(user_id), \
foreign key(song_id) references songs(song_id), \
foreign key(artist_id) references artists(artist_id), \
foreign key(start_time) references time(start_time)\
);\
""")

user_table_create = ("""\
CREATE TABLE IF NOT EXISTS users (\
user_id integer NOT NULL, \
first_name varchar NOT NULL, \
last_name varchar NOT NULL, \
gender CHAR(1), \
level varchar, \
primary key(user_id)\
);\
""")

song_table_create = ("""\
CREATE TABLE IF NOT EXISTS songs (\
song_id varchar NOT NULL, \
title varchar NOT NULL, \
artist_id varchar NOT NULL, \
year integer, \
duration double precision NOT NULL, \
primary key(song_id)\
);\
""")

artist_table_create = ("""\
CREATE TABLE IF NOT EXISTS artists (\
artist_id varchar NOT NULL, \
name varchar NOT NULL, \
location varchar, \
latitude double precision, \
longitude double precision, \
primary key(artist_id)\
);\
""")

time_table_create = ("""\
CREATE TABLE IF NOT EXISTS time (\
start_time timestamp NOT NULL, \
hour smallint NOT NULL, \
day smallint NOT NULL, \
week smallint NOT NULL, \
month smallint NOT NULL, \
year smallint NOT NULL, \
weekday smallint NOT NULL, \
primary key(start_time)\
);\
""")

# STAGING TABLES

staging_events_copy = ("""\
copy staging_events from '{s3_url}'
credentials 'aws_iam_role={aws_iam_role}'
region 'us-west-2'
JSON '{log_json_path}'
""").format(
    s3_url = config['S3']['LOG_DATA'],
    aws_iam_role = config['IAM_ROLE']['ARN'],
    log_json_path = config['S3']['LOG_JSONPATH']
)


staging_songs_copy = ("""
copy staging_songs from '{s3_url}' \
credentials 'aws_iam_role={aws_iam_role}' \
region 'us-west-2' \
JSON 'auto'; \
""").format(
    s3_url = config['S3']['SONG_DATA'],
    aws_iam_role = config['IAM_ROLE']['ARN'],
)

# FINAL TABLES

songplay_table_insert = ("""\
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
SELECT \
    timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, \
    se.userid as user_id, \
    se.level, \
    ss.song_id, \
    ss.artist_id, \
    se.sessionid as session_id, \
    se.location, \
    se.useragent as user_agent \
FROM staging_events se \
JOIN staging_songs ss \
ON se.song = ss.title AND se.artist = ss.artist_name \
WHERE se.page='NextSong';\
""")

user_table_insert = ("""\
INSERT INTO users (user_id, first_name, last_name, gender, level) \
SELECT DISTINCT(se.userid) as user_id, se.firstname as first_name, se.lastname as last_name, se.gender, se.level \
FROM staging_events se \
WHERE se.ts = (\
SELECT max(se2.ts) FROM staging_events se2 WHERE se.userid = se2.userid);\
""")

song_table_insert = ("""\
INSERT INTO songs (song_id, title, artist_id, year, duration) \
SELECT DISTINCT(ss.song_id), ss.title, ss.artist_id, ss.year, ss.duration \
FROM staging_songs ss;\
""")

artist_table_insert = ("""\
INSERT INTO artists (artist_id, name, location, latitude, longitude) \
SELECT DISTINCT(ss.artist_id), ss.artist_name as name, ss.artist_location as location, ss.artist_latitude as latitude, artist_longitude as longitude \
FROM staging_songs ss;\
""")

time_table_insert = ("""\
INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
SELECT \
    DISTINCT(start_time), \
    EXTRACT(HOUR FROM start_time) AS hour, \
    EXTRACT(DAY FROM start_time) AS day, \
    EXTRACT(WEEK FROM start_time) AS week, \
    EXTRACT(MONTH FROM start_time) AS month, \
    EXTRACT(YEAR FROM start_time) AS year, \
    EXTRACT(DOW FROM start_time) AS weekday \
FROM \
(SELECT DISTINCT(timestamp 'epoch' + se.ts/1000 * interval '1 second') as start_time FROM staging_events se);\
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
