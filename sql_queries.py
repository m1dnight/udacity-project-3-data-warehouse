import configparser

from create_cluster import createConfig

# CONFIG
config = createConfig()

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS "events_table";'
staging_songs_table_drop = 'DROP TABLE IF EXISTS "songs_table";'
songplay_table_drop = 'DROP TABLE IF EXISTS "songplays";'
user_table_drop = 'DROP TABLE IF EXISTS "users";'
song_table_drop = 'DROP TABLE IF EXISTS "songs";'
artist_table_drop = 'DROP TABLE IF EXISTS "artists";'
time_table_drop = 'DROP TABLE IF EXISTS "time";'

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE "events_table"
(
    "artist"        character varying(max),
    "auth"          character varying(max),
    "firstName"     character varying(max),
    "gender"        character varying(max),
    "itemInSession" character varying(max),
    "lastName"      character varying(max),
    "length"        double precision,
    "level"         character varying(max),
    "location"      character varying(max),
    "method"        character varying(32),
    "page"          character varying(max),
    "registration"  numeric(38, 0),
    "sessionId"     character varying(max),
    "song"          character varying(max),
    "status"        integer,
    "ts"            numeric(38, 0),
    "userAgent"     character varying(max),
    "userId"        integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE "songs_table"
(
    "artist_id"        character varying(120) NOT NULL,
    "artist_latitude"  double precision,
    "artist_location"  character varying(max),
    "artist_longitude" double precision,
    "artist_name"      character varying(max),
    "duration"         double precision,
    "num_songs"        numeric(38, 0),
    "song_id"          character varying(120) unique,
    "title"            character varying(max),
    "year"             numeric(38, 0)
);
""")

songplay_table_create = ("""
CREATE TABLE songplays
(
    songplay_id int8 identity (0,1)    not null unique,
    start_time  int8 references time,
    user_id     character varying(max) references users,
    level       character varying(max),
    song_id     character varying(max) not null references songs,
    artist_id   character varying(max) not null references artists,
    session_id  character varying(max) not null,
    location    character varying(max),
    user_agent  character varying(max),
    primary key (songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE users
(
    user_id    integer not null unique,
    first_name character varying(max),
    last_name  character varying(max),
    gender     character varying(max),
    level      character varying(max),
    primary key (user_id)
);
""")

song_table_create = ("""
CREATE TABLE songs
(
    song_id   character varying(max) not null unique,
    title     character varying(max) not null,
    artist_id character varying(max) not null,
    year      numeric(4, 0),
    duration  double precision,
    primary key (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE artists
(
    artist_id character varying(max) not null unique,
    name      character varying(max),
    location  character varying(max),
    lattitude double precision,
    longitude double precision,
    primary key (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE time
(
    start_time timestamp     not null unique,
    hour       numeric(2, 0) not null,
    day        numeric(2, 0) not null,
    week       numeric(2, 0) not null,
    month      numeric(2, 0) not null,
    year       numeric(4, 0) not null,
    weekday    numeric(1, 0) not null,
    primary key (start_time)
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY events_table FROM 's3://udacity-dend/log_data/' credentials 'aws_iam_role={}' FORMAT AS JSON 'auto ignorecase';    
""").format(config.get("ARN"))

staging_songs_copy = ("""
COPY songs_table FROM 's3://udacity-dend/song_data/' credentials 'aws_iam_role={}' FORMAT AS JSON 'auto ignorecase';    
""").format(config.get("ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select ts,
       userid,
       level,
       song_id,
       artists.artist_id,
       sessionid,
       et.location,
       useragent
from events_table et
         join artists on artists.name = et.artist
         join songs on songs.artist_id = artists.artist_id
where et.page like 'NextSong';
""")

user_table_insert = ("""
-- Insert all free users first.
INSERT INTO users (user_id, first_name, last_name, gender, level)
select distinct userid, firstname, lastname, gender, level
from events_table
where userid not like ''
  and level like 'free';

-- Delete all user ids that also have a paid level.
delete
from users
where user_id in (select distinct userid from events_table where level like 'paid');

-- Insert all users that are paid.
INSERT INTO users (user_id, first_name, last_name, gender, level)
select distinct userid, firstname, lastname, gender, level
from events_table
where userid not like ''
  and level like 'paid';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
from songs_table;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM songs_table;
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select ts                       as start_time,
       extract(hour from ts)    as hour,
       extract(day from ts)     as day,
       extract(week from ts)    as week,
       extract(month from ts)   as month,
       extract(year from ts)    as year,
       extract(weekday from ts) as weekday
from (SELECT DISTINCT (TIMESTAMP 'epoch' + ts * INTERVAL '0.001 second') as ts from events_table) as ett;
""")

# QUERY LISTS

# create_table_queries = [staging_songs_table_create, staging_events_table_create]
# drop_table_queries = [staging_songs_table_drop, staging_events_table_drop]
# copy_table_queries = [staging_songs_copy, staging_events_copy]
# insert_table_queries = []
create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,
                        songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = []
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,
                        songplay_table_insert]
