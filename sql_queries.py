import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN=config.get("IAM_ROLE","ARN")

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE "staging_events_table" (
    artist          varchar(256) distkey,
    auth            varchar(25),
    firstName       varchar(128),
    gender          varchar(2),
    iteminSession   integer,
    lastName        varchar(128),
    length          float8,
    level           varchar(10),
    location        varchar(128),
    method          varchar(5),
    page            varchar(22),
    registration    bigint,
    sessionid       integer,
    song            varchar,
    status          integer,
    ts              timestamp,
    useragent       varchar(256),
    userId     	    integer
)
SORTKEY ("artist", "song");
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs_table" (
    num_songs           varchar(25), 
    artist_id           varchar(25),
    artist_longitude    float,
    artist_latitude     float,
    artist_location     varchar(256),
    artist_name         varchar(256) distkey,
    song_id             varchar(32),
    title               varchar(256),
    duration            float8,
    year                integer
)
SORTKEY ("artist_name", "title");
""")

songplay_table_create = ("""
CREATE TABLE "songplays" (
    songplay_id         bigint          IDENTITY(0,1)   not null,
    start_time          timestamp       not null sortkey,
    user_id             varchar(64)     not null,
    level               varchar(16)     not null,
    song_id             varchar(64)     not null,
    artist_id           varchar(64)     not null,
    session_id          varchar(64)     not null,
    location            varchar(256)    not null,
    user_agent          varchar(256)    not null
)
DISTSTYLE ALL;
""")

user_table_create = ("""
CREATE TABLE "users" (
    user_id             varchar(64)     not null,
    first_name          varchar(256)    not null,
    last_name           varchar(256)    not null,
    gender              varchar(8)      not null,
    level               varchar (16)    not null
)
DISTSTYLE ALL
SORTKEY ("level", "user_id");
""")

song_table_create = ("""
CREATE TABLE "songs" (
    song_id             varchar(64)     not null sortkey,
    title               varchar(256)    not null,
    artist_id           varchar(64)     not null distkey,
    year                integer         not null,
    duration            float8          not null
);
""")

artist_table_create = ("""
CREATE TABLE "artists" (
    artist_id           varchar(64)     not null sortkey,
    name                varchar(256)    not null,
    location            varchar(256)    not null,
    lattitude           float,
    longitude           float
);
""")

time_table_create = ("""
CREATE TABLE "time" (
    start_time          timestamp       not null,
    hour                integer         not null,
    day                 integer         not null,
    week                integer         not null,
    month               integer         not null,
    year                integer         not null,
    weekday             boolean         not null
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events_table from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON '{}'
    TIMEFORMAT as 'epochmillisecs';
""".format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH))


staging_songs_copy = ("""
    copy staging_songs_table from '{}'
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    json 'auto';
""".format(SONG_DATA, DWH_ROLE_ARN))

# FINAL TABLES

songplay_table_insert = ("""
INSERT into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    ts,
    userId,
    level,
    song_id,
    artist_id,
    sessionid,
    a.location,
    useragent
from staging_events_table a,
staging_songs_table b
where a.song = b.title
and a.artist = b.artist_name
and page = 'NextSong'
""")

user_table_insert = ("""
INSERT into users(user_id,first_name,last_name,gender,level)
SELECT distinct userId AS user_id,firstName as first_name,lastNAme AS last_name,gender,level from staging_events_table
Where userId is not null;
""")

song_table_insert = ("""
INSERT into songs(song_id,title,artist_id,year,duration)
SELECT distinct song_id,title,artist_id,year,duration from staging_songs_table;
""")

artist_table_insert = ("""
INSERT into artists(artist_id, name, location, lattitude, longitude)
SELECT distinct artist_id, artist_name as name, coalesce(artist_location,'') as location, artist_latitude as lattitude, artist_longitude as longitude from staging_songs_table;
""")

time_table_insert = ("""
INSERT into time(start_time, hour, day, week, month, year, weekday)
SELECT 
    DISTINCT
    ts as start_time,        
    extract(hour from ts) as hour,
    extract(day from ts) as day,
    extract(week from ts) as week, 
    extract(month from ts) as month,
    extract(year from ts) as year,
    extract(weekday from ts) as weekday
from staging_events_table a
where ts is NOT NULL
Order by a.ts desc;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]