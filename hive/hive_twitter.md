# Apache Hive 2 

Topics
- Hive
- Complex data type
- collection functions
- advanced string functions
- UDAF
- UDTF
- Sqoop
- moving data between relational database (mysql) and hive
## Hive CLI
### Current Database in Use
```shell
set hive.cli.print.current.db=true;
```
or hive-site.xml
```xml
<property>
<name>hive.cli.print.current.db</name>
<value>true</value>
</property>
```

## Dataset
```shell
hadoop fs -put /path/to/twitter_data/full_text.txt /path/to/hdfs/dir
```
To avoid confusion, please always include database name 'twitter.' as part of your hive table name. If you 
don't specify the database name while you're not in the twitter database (use twitter), you will not find the
the corresponding table. By default you're in a database called "default" 
e.g.,  twitter.full_text

```shell
hive> show databases;
OK
default
demo
foodmart
twitter
xademo
Time taken: 0.046 seconds, Fetched: 5 row(s)
```
--------------------------------------------------------------------
-- load geo-tagged tweets as external hive table

-- Note: you can skip this if you already have 
-- twitter.full_text_ts table created from previous lab
--------------------------------------------------------------------

-- create and load tweet data as external table
drop table twitter.full_text;
create external table twitter.full_text (                                                   
          id string, 
          ts string, 
          lat_lon string,
          lat string, 
          lon string, 
          tweet string)
row format delimited 
fields terminated by '\t'
location '/user/twitter/full_text' ;     
-- note: you may have your data in a different hadoop directory and that's fine!

-- convert timestamp

drop table twitter.full_text_ts;

create table twitter.full_text_ts as
select id, cast(concat(substr(ts,1,10), ' ', substr(ts,12,8)) as timestamp) as ts, lat, lon, tweet
from twitter.full_text;


-----------------------------------------------
--- Complext Data Types -- Map/Array/Struct 
-----------------------------------------------

-- Creating table and load data with complex types
-- NOTE: because the twitter data is not in a proper format,
--       we need to prepare the data so that we can try 
--       complex type exercise


-- create a temporary table schema
drop table twitter.full_text_ts_complex_tmp;
create external table twitter.full_text_ts_complex_tmp (
                       id string,
                       ts timestamp,
                       lat float,
                       lon float,
                       tweet string,
                       location_array string, 
                       location_map string,
                       tweet_struct string
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/user/twitter/full_text_ts_complex';

-- load transformed data into the temp table

insert overwrite table twitter.full_text_ts_complex_tmp
select id, ts, lat, lon, tweet, 
       concat(lat,',',lon) as location_array,
       concat('lat:', lat, ',', 'lon:', lon) as location_map, 
       concat(regexp_extract(lower(tweet), '(.*)@user_(\\S{8})([:| ])(.*)',2), ',', length(tweet)) as tweet_struct
from twitter.full_text_ts;

select * from twitter.full_text_ts_complex_tmp limit 3;


-- NOTE: we can drop the tmp hive table because all we need is 
--       the HDFS file '/user/twitter/full_text_ts_complex'

drop table twitter.full_text_ts_complex_tmp;

-- To prove that the directory is still there... 

dfs -ls /user/twitter/full_text_ts_complex;




-- Reload the temp file using complex types instead of strings
-- NOTE: you specify the complex type when you create the table schema

drop table twitter.full_text_ts_complex;
create external table twitter.full_text_ts_complex (
       id                 string,
       ts                 timestamp,
       lat                float,
       lon                float,
       tweet              string,
       location_array     array<float>,
       location_map       map<string, string>,
       tweet_struct       struct<mention:string, size:int>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
LOCATION '/user/twitter/full_text_ts_complex';

select * from twitter.full_text_ts_complex limit 3;


-----------------------------------------------
-- Hive Collection Functions
-----------------------------------------------

-- Create complex type directly using map(), array(), struct() functions

select id, ts, lat, lon, 
       array(lat, lon) as location_array, 
       map('lat', lat, 'lon', lon)  as location_map, 
       named_struct('lat', lat, 'lon',lon) as location_struct
from twitter.full_text_ts
limit 10;


-- Work with collection functions
   -- extract element from arrays/maps using indexing
   -- extract element from struct using 'dot' notation

select location_array[0] as lat, 
       location_map['lon'] as lon, 
       tweet_struct.mention as mention,
       tweet_struct.size as tweet_length
from twitter.full_text_ts_complex
limit 5;

-- Work with collection functions
   -- extract all keys/values from maps
   -- get number of elements in arrays/maps

select size(location_array), sort_array(location_array),
       size(location_map), map_keys(location_map), map_values(location_map)
from twitter.full_text_ts_complex
limit 5;


-----------------------------------------------
-- Hive Advanced String Functions
-----------------------------------------------

-- sentences function
select sentences(tweet)
from twitter.full_text_ts
limit 10;

-- ngrams function
   -- *** find popular bigrams ***
select ngrams(sentences(tweet), 2, 10)
from twitter.full_text_ts
limit 50;

-- ngrams function with explode()
   -- *** find popular bigrams ***
select explode(ngrams(sentences(tweet), 2, 10))
from twitter.full_text_ts
limit 50;

-- context_ngrams function
   -- *** find popular word after 'I need' bi-grams ***
select explode(context_ngrams(sentences(tweet), array('I', 'need', null), 10))
from twitter.full_text_ts
limit 50;

-- context_ngrams function
   -- *** find popular tri-grams after 'I need' bi-grams ***
select explode(context_ngrams(sentences(tweet), array('I', 'need', null, null, null), 10))
from twitter.full_text_ts
limit 50;

-- str_to_map
   -- map a string to map complex type
select str_to_map(concat('lat:',lat,',','lon:',lon),',',':') 
from twitter.full_text_ts
limit 10;




-----------------------------------------------
-- Aggregation Functions (UDAF)
-----------------------------------------------

-- MIN function
   -- *** Find twitter user who reside on the west most point of U.S. ***
   -- You can visualize it using the map tool at: http://www.darrinward.com/lat-long/?id=461435

select distinct lat, lon
from twitter.full_text_ts_complex x
where cast(x.lon as float) IN (select min(cast(y.lon as float)) as lon from twitter.full_text_ts y);

-- SAVE the output of the above query in a directory on HDFS

INSERT OVERWRITE DIRECTORY '/user/lab/westUS'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
SELECT DISTINCT lat, lon
FROM twitter.full_text_ts_complex x
WHERE cast(x.lon as float) IN (select min(cast(y.lon as float)) as lon from twitter.full_text_ts y);

-- PERCENTILE_APPROX function (works with DOUBLE type)
   -- *** Find twitter users from north west part of U.S. ***
   -- You can visualize it using the map tool: http://www.darrinward.com/lat-long/?id=461435

select percentile_approx(cast(lat as double), array(0.9))
from twitter.full_text_ts_complex;   --  41.79976907219686


select percentile_approx(cast(lon as double), array(0.1))
from twitter.full_text_ts_complex;   --  -117.06394155417728

select distinct lat, lon
from twitter.full_text_ts_complex
where cast(lat as double) >= 41.79976907219686 AND
      cast(lon as double) <= -117.06394155417728
limit 10;


-- HISTOGRAM_NUMERIC
   -- *** Bucket U.S. into 10x10 grids using histogram_numeric ***
   -- get 10 variable-sized bins for latitude and longitude first
   -- use cross-join to create the grid
   -- visualize the result using the map tool at: http://www.darrinward.com/lat-long/?id=461435

-- get 10 variable-sized bins and their counts
select explode(histogram_numeric(lat, 10)) as hist_lat from twitter.full_text_ts_complex;

select explode(histogram_numeric(lon, 10)) as hist_lon from twitter.full_text_ts_complex;

-- extract lat/lon points from the histogram output (struct type) separately 
select t.hist_lat.x from (select explode(histogram_numeric(lat, 10)) as hist_lat from twitter.full_text_ts_complex) t;

select t.hist_lon.x from (select explode(histogram_numeric(lon, 10)) as hist_lon from twitter.full_text_ts_complex) t;


-- write a nested query the cross-joins the 10x10 lat/lon points 

select t1.lat, t2.lon
from 
    (select t.hist_lat.x as lat 
        from (select explode(histogram_numeric(lat, 10)) as hist_lat 
                from twitter.full_text_ts_complex
                where lat>=24.9493 AND lat<=49.5904 AND lon>=-125.0011 and lon<=-66.9326) t
    ) t1
JOIN
    (select t.hist_lon.x as lon 
        from (select explode(histogram_numeric(lon, 10)) as hist_lon 
                from twitter.full_text_ts_complex
                where lat>=24.9493 AND lat<=49.5904 AND lon>=-125.0011 and lon<=-66.9326) t
    ) t2
;


-----------------------------------------------
-- Table-generating Functions (UDTF)
-----------------------------------------------

-- explode() function and lateral_view
   -- explode() function is often used with lateral_view
   -- we extracted twitter mentions from tweets in earlier exercises. You've probably noticed 
   -- that it's not optimal soultion because the query we wrote didn't handle multiple
   -- mentions. It only extract the very first mention. A better approach is to tokenize
   -- the tweet first and then explode the tokens into rows and extract mentions from each token

drop table twitter.full_text_ts_complex_1;
create table twitter.full_text_ts_complex_1 as
select id, ts, location_map, tweet, regexp_extract(lower(tweet_element), '(.*)@user_(\\S{8})([:| ])(.*)',2) as mention
from twitter.full_text_ts_complex
lateral view explode(split(tweet, '\\s')) tmp as tweet_element
where trim(regexp_extract(lower(tweet_element), '(.*)@user_(\\S{8})([:| ])(.*)',2)) != "" ;

select * from twitter.full_text_ts_complex_1 limit 10;



-- collect_set function (UDAF)
   -- collect_set() is a UDAF aggregation function.. we run the query at this step 
   -- from the previous step, we get all the mentions in the tweets but if a user
   -- has multiple mentions in the same tweet, they are in different rows. 
   -- To transpose all the mentions belonging to the same tweet/user, we can use
   -- the collect_set and group by to transpose the them into an array of mentions

create table twitter.full_text_ts_complex_2 as
select id, ts, location_map, tweet, collect_list(mention) as mentions
from twitter.full_text_ts_complex_1
group by id, ts, location_map, tweet;

describe twitter.full_text_ts_complex_2;

select * from twitter.full_text_ts_complex_2 
where size(mentions) > 5
limit 10;


-----------------------------------------------
-- Nested Queries
-----------------------------------------------

-- Nested queries
   -- *** tweets that have a lot of mentions ***

select t.*
from (select id, ts, location_map, mentions, size(mentions) as num_mentions 
      from twitter.full_text_ts_complex_2) t
order by t.num_mentions desc
limit 10;





-----------------------------------------------------
-- Sqoop
-- Moving data between relational database and hadoop
-----------------------------------------------------

-- *** 
--     create a full_text_mysql table in mysql under twitter 
--     database and then sqoop the mysql table to hive 
-- ***

-- From your linux prompt, launch mysql
[root@sandbox etc]# mysql

-- In mysql prompt...
mysql> show databases;
mysql> create database twitter;


-- create mysql table
mysql> create table twitter.full_text_mysql (id varchar(20), ts varchar(20), location varchar(20), lat varchar(20), lon varchar(20), tweet varchar(300));

-- load twitter data into mysql and check whether the laod is successful
msyql> LOAD DATA LOCAL INFILE '/home/lab/full_text.txt' INTO TABLE twitter.full_text_mysql FIELDS TERMINATED BY '\t';
mysql> select * from twitter.full_text_mysql limit 3;
mysql> quit;

-- From linux prompt, use sqoop to transfer full_table from mysql to hive

[root@sandbox etc]#  sqoop import -m 1 --connect jdbc:mysql://0.0.0.0:3306/twitter --username=root --password= --table full_text_mysql --driver com.mysql.jdbc.Driver --columns "id, ts, location" --map-column-hive id=string,ts=string,location=string --hive-import --fields-terminated-by '\t'  --hive-table twitter.full_text_mysql  --warehouse-dir /user/twitter/full_text_mysql





