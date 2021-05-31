create external table titleBasicsExternal (
    tconst string,
    titleType string,
    primaryTitle string,
    originalTitle string,
    isAdult boolean,
    startYear smallint,
    endYear smallint,
    runtimeMinutes smallint,
    genres array<string>
)
row format delimited
    fields terminated by '\t'
    collection items terminated by ','
    lines terminated by '\n'
    NULL defined as '\\N'
stored as textfile
location 'hdfs:///titleBasics'
tblproperties ("skip.header.line.count"="1");

create table titleBasics (
     tconst string,
     primaryTitle string,
     originalTitle string,
     isAdult boolean,
     startYear smallint,
     endYear smallint,
     runtimeMinutes smallint,
     genres array<string>
)
partitioned by (titleType string)
stored as parquet;

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table titleBasics partition(titleType)
    select tconst, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres, titleType from titleBasicsExternal;