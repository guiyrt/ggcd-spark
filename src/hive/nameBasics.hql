create external table nameBasicsExternal (
    nconst string,
    primaryName string,
    birthYear smallint,
    deathYear smallint,
    primaryProfession array<string>,
    knownForTitles array<string>
)
row format delimited
    fields terminated by '\t'
    collection items terminated by ','
    lines terminated by '\n'
    NULL defined as '\\N'
stored as textfile
location 'hdfs:///nameBasics'
tblproperties ("skip.header.line.count"="1");

create table nameBasics (
    nconst string,
    primaryName string,
    birthYear smallint,
    deathYear smallint,
    primaryProfession array<string>,
    knownForTitles array<string>
)
stored as parquet;

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table nameBasics select nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles from nameBasicsExternal;