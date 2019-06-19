# Red Jam
Data warehouse infrastructure as code and an ETL pipeline to analyze a song app's listening data using Amazon Redshift.

## Overview
#### Purpose
The purpose of this project is to create a data warehouse allowing analysts to run quick ad hoc queries and developers to build performant BI dashboards. The primary objective of the businesses analysts in this context is to understand user song play behavior. What songs and artists are they listening to most frequently? How often do they listen? How does artist popularity change over time? These questions and more can be explored using the data warehouse.
#### Schema
The data warehouse contains two staging tables which are used to populate a star schema. The table `staging_events` contains user song play data, whereas `staging_songs` contains song data.

In the star schema, the `songplays` table is the fact table. The dimension tables are `songs`, `artists`, `users`, and `time`.

The field `song_id` is used as the `DISTKEY` for `songplays` and `songs`.  This allows the data to be distributed on the cluster for these two tables, while allowing for fast joins between them.  

The field `start_time` is used as the `SORTKEY` on the `songplay` table to make filtering or sorting by time much faster.

The tables `artists` and `users` use a `DISTSTYLE` of `ALL`, copying the data to each node on the cluster. This is feasible because these tables are relatively small. Using a `DISTSTYLE` of `ALL` allows these tables to be joined to the others faster.
#### Future Work
Add `UPSERT` statements to `sql_queries.py` and build into the ETL pipeline.

## Getting Started

#### Config
Create and complete `dwh.cfg`:
```
[CLUSTER]
HOST=''
IDENTIFIER=
DBNAME=
USER=
PASSWORD=
PORT=

CLUSTER_TYPE=multi-node
NUM_NODES=4
NODE_TYPE=dc2.large
IAM_ROLE_NAME=

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://udacity-dend/log-data'
LOG_JSON_PATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song-data'
```


#### Infrastructure
The `infrastructure.py` file uses the configurations spelled out in `dwh.cfg` to build the infrastructure of the data warehouse in Amazon Redshift. To create the Redshift cluster, run `$ python infrastructure.py --build true`. To delete the cluster, run `$ python infrastructure.py --delete true`.

**Note: You'll need to set `DW_AWS_ACCESS_KEY_ID` and `DW_AWS_SECRET_ACCESS_KEY` as environment variables.  These credentials should belong to a user with a policy allowing full Redshift access.**

#### Populate the Data Warehouse
Run `$ python create_tables.py` to create the staging tables, as well as teh star schema. Then run `$ python etl.py`.  This last step will take quite some time to finish due to the amount of data.
