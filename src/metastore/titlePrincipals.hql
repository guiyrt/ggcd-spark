create external table titlePrincipalsExternal (
    tconst string,
    ordering smallint,
    nconst string,
    category string,
    job string,
    characters array<string>
)
row format delimited
    fields terminated by '\t'
    collection items terminated by ','
    lines terminated by '\n'
    NULL defined as '\\N'
stored as textfile
location 'hdfs:///titlePrincipals'
tblproperties ("skip.header.line.count"="1");

create table titlePrincipals (
     tconst string,
     ordering smallint,
     nconst string,
     category string,
     job string,
     characters array<string>
)
stored as parquet;

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table titlePrincipals select tconst, ordering, nconst, category, job, characters from titlePrincipalsExternal;