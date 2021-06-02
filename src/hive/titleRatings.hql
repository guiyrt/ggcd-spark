create external table titleRatingsExternal (
    tconst string,
    averageRating decimal(10,1),
    numVotes integer
)
row format delimited
    fields terminated by '\t'
    lines terminated by '\n'
    NULL defined as '\\N'
stored as textfile
location 'hdfs:///titleRatings'
tblproperties ("skip.header.line.count"="1");

create table titleRatings (
   tconst string,
   averageRating decimal(10,1),
   numVotes integer
)
stored as parquet;

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table titleRatings select tconst, averageRating, numVotes from titleRatingsExternal;