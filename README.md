# Data Warehouse

The third Udacity DEND project


## Run me 

add the correct values to dwh.cfg. Instructions below in dwh.cfg description

**command line**
```
python create_tables.py
python etl.py
```

**ipython notebook**
```
%run -i 'create_tables'
%run -i 'etl'
```

## Project Files

### dwh.cfg

This file contains the configuration informtion that is needed to communicate with the AWS Redshift Database. The cluster related information is related to the AWS Redshift provision. 

It is important to add the details below to the dwh.cfg file. The information needs to be added **with no quotes**

```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=
```
It is important to add that a configuration for IAM_ROLE was added as shown below. This is used for passing credentials for a copy from S3 statement.

```
[IAM_ROLE]
ARN=
```

The S3 parameters below provides the location of the files with teh data to be loaded into the staging tables 

```
[S3]
LOG_DATA=s3://udacity-dend/log-data
LOG_JSONPATH=s3://udacity-dend/log_json_path.json
SONG_DATA=s3://udacity-dend/song_data/
```

The file has been addded to the project without the private sensitive information. 

The file could have been omitted with a .gitignore


### sql_queries.py

This contains queries that are used for the following: 
* Drop all the tables from the database associated with this snowflake schema
* Create the 7 tables that are required the ETL process
* Load the events and songs staging tables using copy staments
* Load data into the Snowflake Scheme from the staging table using NSERT INTO SELECT statements

These queries are used in the following python files create_table.py and etl.py


### create_tables.py

This can be viewed as the file for resetting the database. 

This file serves 2 purposes:
* drop tables from the database
* create the table that are needed in the database 

The queries that are used to perform that actions described above use queries defind in sql_queries.py


### etl.py

This file is resposible from ETL process. It copies data from the S3 into Redshift before loading it into a star scheme data warehouse. 

This is broken down into two steps:
* Using a copy statement to move data from S3 into Reshift staging tables
* Transform and load data onto our star schema's fact and dimension table

The queries that are used to perform that actions described above use queries defind in sql_queries.py


### Scrapbook.ipynb

This is the Scrapbook that I used to create my proof of concept for the Project. It has been cleaned up, refined and commented. 


## Key selection for Tables


### Dimension and Fact Table Row Counts

table_schema|table_name|rows
--- | --- | ---
public|songs|14896
public|artists|10025
public|time|8023
public|songplays|333
public|users|105



### staging_events_table

The distibution key selected was artist because this is going to used as a joining key for loading data into the songplays fact table.

The sort key was choosen as artist and song because these are used join keys. 

### staging_songs_table

The distibution key selected was artist_name because this is going to used as a joining key for loading data into the songplays fact table.

The sort key was choosen as artist_name and title because these are used join keys. 

### songplays

I used starttime as a sort key because I anticipate using alot of time-based analysis to measure Growth Accounting (Daily, weekly and Monthy Active Users) as well as time spent on the platform.

DistALL was chosen because of the size of songplays was since it is small copying it to all of the nodes will remove data shuffling involving this table.

If the data scales and this is no longer a suitable strategy, we can revisit ing the future at that point.

**Joining staging_events_table and staging_songs_table**
Two keys were used to join the events and songs staging tables. This took match (song, artist) in events to (title, artist_name) to ensure unique artist and song combinations because song and artist names are not unique. Joining by song_id or artist_id would have been the optimal solution.

### users

The sortkey are the user_id

Dist all has been chosen because of the size of this table 

### artists

The sortkey is artist_id because it could be used in joins with songplays and songs tables

### songs

The sortkey was selected as song_id because it is used to join with the songplays and artists tables

The distkey was selected as artist_id because arts can have multiple songs and this will be key in doing artist related analysis

### time

The sort key was selected as start_time due to it being used to join with the songplays fact table


