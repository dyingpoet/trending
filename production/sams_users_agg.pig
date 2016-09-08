/* 
REGISTER /opt/mapr/pig/pig-0.11.2/contrib/piggybank/java/piggybank.jar 
REGISTER /opt/mapr/pig/pig-0.13/contrib/piggybank/java/piggybank.jar
REGISTER /home/ucidscs/scs/lib/joda-time-1.6.jar
*/
REGISTER /opt/mapr/pig/pig-0.14/contrib/piggybank/java/piggybank.jar


SET DEFAULT_PARALLEL 400;
SET mapred.output.compress 'true';
SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';

DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix(); 

Browse = LOAD '$INPUT_BROWSE' USING PigStorage('\u0001') AS (
    vid: chararray,
    mid: chararray, 
    actionType: chararray,
    catalog_item_id: chararray,
    bucket: chararray,
    visit_date:chararray
);

Browse = FOREACH Browse GENERATE vid, mid, actionType, catalog_item_id, bucket, CONCAT(CONCAT(CONCAT(SUBSTRING(visit_date, 0, 4), '-'), CONCAT(SUBSTRING(visit_date, 4, 6), '-')), SUBSTRING(visit_date, 6, 8)) AS visit_date;

Purchase = LOAD '$INPUT_PURCHASE' USING PigStorage('\u0001') AS (
    vid: chararray,
    mid: chararray, 
    actionType: chararray,
    catalog_item_id: chararray,
    bucket: chararray,
    visit_date: chararray
);

S0 = UNION Browse, Purchase;

S0 = FOREACH S0 GENERATE (SIZE(vid) == 0 ? '#' : vid) AS vid, (SIZE(mid) == 0 ? '#' : mid) AS mid, actionType, catalog_item_id, bucket, visit_date;

S0 = FILTER S0 BY SIZE(vid) < 20 AND SIZE(mid) < 18;

S1 = FOREACH S0 GENERATE CONCAT(CONCAT(vid,'-'), mid) as uid, actionType, catalog_item_id, bucket, ISOToUnix(CustomFormatToISO(visit_date,'yyyy-MM-dd')) AS visit_epoch;

S2 = GROUP S1 BY (uid, actionType, bucket, visit_epoch);

S3 = FOREACH S2 GENERATE FLATTEN(group) AS (uid, actionType, bucket, visit_epoch), COUNT(S1) AS visits; 

S3 = FILTER S3 BY visits > 0 ;

S4 = GROUP S3 BY (uid, bucket, visit_epoch);

S5 = FOREACH S4 GENERATE FLATTEN(group) AS (uid, bucket, visit_epoch), S3.(actionType, visits) AS smry;

--store S5 into '/user/jli21/Databases/scs_model.db/scs_user_group' using PigStorage('|');
--S5 = load '/user/jli21/Databases/scs_model.db/scs_user_group' using PigStorage('|') as (uid:chararray, bucket:chararray, visit_epoch:chararray, smry:chararray);

define parse_action  `/usr/bin/python parse_action.py`
    input (stdin using PigStreaming('|'))
    output (stdout using PigStreaming('|'))
    ship('$TaskDir/parse_action.py','$TaskDir/bag.py');

S6 = STREAM S5 THROUGH parse_action AS (uid:chararray, id:chararray, visit_epoch:chararray, views:int, add_to_carts:int, onlineTransactions:int, clubTransactions:int);

S7a = FOREACH S6 GENERATE uid, id, visit_epoch, views, add_to_carts, onlineTransactions + clubTransactions AS transactions;
/*
S7 = FOREACH S6 GENERATE uid, id, visit_epoch, views, add_to_carts, onlineTransactions + clubTransactions AS transactions;
store S7 into '/hive/jli21.db/scs_model.db/uid_data' using PigStorage('|');
g7 = group S7 by uid;
c7 = foreach g7 generate group as uid, COUNT(S7) as cnt;
store c7 into '/hive/jli21.db/scs_model.db/uid_cnt' using PigStorage('|');
S7a = load '/hive/jli21.db/scs_model.db/uid_data' using PigStorage('|') as (uid:chararray, id:chararray, visit_epoch:chararray, views:int, add_to_carts:int, transactions:int);
*/

g7 = group S7a by uid;
c7 = foreach g7 generate group as uid, COUNT(S7a) as cnt;
S7j = JOIN S7a BY uid, c7 BY uid;
S7f = FILTER S7j BY cnt <= 3000000;

--S7 = filter S7 BY (uid !='10134110691120711' AND uid !='10134100734636590' AND uid !='10134260522488386' AND uid !='10134120522488386' AND uid !='10134100765799390' AND uid !='10134100745065359' AND uid !='10134100675174791' AND uid !='10134100693747610' AND uid !='10134100765802723' AND uid !='10134100823686191' AND uid !='10134100810822288' AND uid !='10134100816343818' AND uid !='10134100729296863' AND uid !='10134310522488386' AND uid !='10134380544345432' AND uid !='10134210634943716' AND uid !='10134100521842674' AND uid !='10134100752945873' AND uid !='10134100765800693' AND uid !='10134100816342653');

/*
10134110691120711|595778555
10134100734636590|228173736
10134260522488386|140394910
10134120522488386|133710676
*/

S7 = FOREACH S7f GENERATE S7a::uid AS uid, id AS id, visit_epoch AS visit_epoch, views AS views, add_to_carts AS add_to_carts, transactions AS transactions;

S8 = GROUP S7 BY (uid, visit_epoch);

S9 = FOREACH S8 GENERATE FLATTEN(group) AS (uid, start_epoch), S7.(id, views, add_to_carts, transactions) AS cat_features;

S10 = GROUP S9 BY uid;

S11 = FOREACH S10 GENERATE FLATTEN(group) AS (visitor_id), S9.(start_epoch, cat_features) AS user_hist_session_features;


STORE S11 INTO '$OUTPUT' USING JsonStorage();
/*
*/


