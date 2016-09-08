add jar ./analytics.jar;
add jar ./scs-udfs-0.0.1-SNAPSHOT.jar;
add jar ./twofish-0.0.1.jar;
add jar ./athena-udfs-0.1.jar;

create temporary function parse_beacon_url as 'analysis.visit.chain.ParseBeaconUrlUDF';
create temporary function decryptMembership as 'com.walmart.scs.udfs.DecryptMembership';
create temporary function decrypt as 'com.walmart.scs.udfs.Decrypt';
create temporary function decode as 'com.walmart.scs.udfs.DecodeURL';

use zzhao3;

alter table order drop if exists partition (dt=${hiveconf:dt}, dthour=${hiveconf:dthour});

insert into table order partition (dt=${hiveconf:dt}, dthour=${hiveconf:dthour})
SELECT b30msc
    ,bsc
    ,btc
    ,vid
    ,cid
    ,sams_hubble_session
    ,req_start_time
    ,page_url
    ,IF(INSTR(page_url, 'www.samsclub.com/sams') > 0 AND INSTR(page_url, '.ip') > 0,
       SUBSTR(SUBSTR(SUBSTR(page_url, INSTR(page_url, 'www.samsclub.com/sams') + 22), INSTR(SUBSTR(page_url, INSTR(page_url, 'www.samsclub.com/sams') + 22), '/') + 1), 1,
       IF(INSTR(SUBSTR(SUBSTR(page_url, INSTR(page_url, 'www.samsclub.com/sams') + 22), INSTR(SUBSTR(page_url, INSTR(page_url, 'www.samsclub.com/sams') + 22), '/') + 1), '.ip') = 0,
       length(SUBSTR(SUBSTR(page_url, INSTR(page_url, 'www.samsclub.com/sams') + 22), INSTR(SUBSTR(page_url, INSTR(page_url, 'www.samsclub.com/sams') + 22), '/') + 1)),
       INSTR(SUBSTR(SUBSTR(page_url, INSTR(page_url, 'www.samsclub.com/sams') + 22), INSTR(SUBSTR(page_url, INSTR(page_url, 'www.samsclub.com/sams') + 22), '/') + 1), '.ip') - 1)), NULL) AS prod_id_from_page_url
    ,refer_url
    ,IF(INSTR(refer_url, 'www.samsclub.com/sams') > 0 AND INSTR(refer_url, '.ip') > 0,
       SUBSTR(SUBSTR(SUBSTR(refer_url, INSTR(refer_url, 'www.samsclub.com/sams') + 22), INSTR(SUBSTR(refer_url, INSTR(refer_url, 'www.samsclub.com/sams') + 22), '/') + 1), 1,
       IF(INSTR(SUBSTR(SUBSTR(refer_url, INSTR(refer_url, 'www.samsclub.com/sams') + 22), INSTR(SUBSTR(refer_url, INSTR(refer_url, 'www.samsclub.com/sams') + 22), '/') + 1), '.ip') = 0,
       length(SUBSTR(SUBSTR(refer_url, INSTR(refer_url, 'www.samsclub.com/sams') + 22), INSTR(SUBSTR(refer_url, INSTR(refer_url, 'www.samsclub.com/sams') + 22), '/') + 1)),
       INSTR(SUBSTR(SUBSTR(refer_url, INSTR(refer_url, 'www.samsclub.com/sams') + 22), INSTR(SUBSTR(refer_url, INSTR(refer_url, 'www.samsclub.com/sams') + 22), '/') + 1), '.ip') - 1)), NULL) AS prod_id_from_refer_url
    ,gpv_p6
    ,evar_19
    ,pv_id
    ,page_type
    ,item_ids
    ,mapped_item_ids
    ,uagent
    ,ts
    ,orderid
    ,ordertype
    ,CAST(item_quantities AS DOUBLE) AS item_quantities
    ,CAST(order_amt AS DOUBLE) AS order_amt
    ,CAST(subtotal AS DOUBLE) AS subtotal
    ,CAST(ordertotal AS DOUBLE) AS ordertotal
    ,query_string
FROM (
SELECT COALESCE(cookie_map['b30msc'], '#') AS b30msc
        ,COALESCE(cookie_map['bsc'], substr(response_headers['set-cookie'][0], locate('bsc=',response_headers['set-cookie'][0])+4,22)) as bsc
        ,COALESCE(cookie_map['btc'], substr(response_headers['set-cookie'][0], locate('btc=',response_headers['set-cookie'][0])+4,22)) as btc
        ,COALESCE(cookie_map ['samsVisitor'], '#') AS vid
        ,COALESCE(decryptMembership(decode(cookie_map['uExp'])),decryptMembership(decode(cookie_map['pilotusercookie'])),'#') as cid
        ,COALESCE(cookie_map['samsHubbleSession'],'#') as sams_hubble_session
        ,COALESCE(request_start_time, cast(get_json_object(line, '$.server.requestStartTime') as bigint)) as req_start_time
        ,COALESCE(decode(query_map['u'],true),'#') as page_url
        ,COALESCE(decode(query_map['r'],true), '#') as refer_url
        ,COALESCE(decode(cookie_map['gpv_p6']), '#') as gpv_p6
        ,COALESCE(decode(cookie_map['evar_19']), '#') as evar_19
        ,COALESCE(decode(query_map['pv_id']), '#') as pv_id
        ,COALESCE(decode(query_map['page_type']), '#') as page_type
        ,COALESCE(decode(query_map['item_ids']), decode(query_map['item_id']), '#') as item_ids
        ,COALESCE(decode(query_map['mapped_item_ids']), '#') as mapped_item_ids
        ,get_json_object(request_headers['user-agent'][0], '$.valueList[0]') as uagent
        ,parse_beacon_url(query_string, 'ts', true) AS ts
        ,query_map['order_id'] AS orderid
        ,query_map['orderType'] AS ordertype
        ,query_map['item_quantities'] AS item_quantities
        ,query_map['order_amt'] AS order_amt
        ,query_map['order_sub_total'] as subtotal
        ,query_map['order_total'] as ordertotal
        ,query_string
    FROM hubble_by_host where partition_host='beacon.samsclub.com' and partition_epoch_hourtenth>=${hiveconf:start_partition} and partition_epoch_hourtenth<${hiveconf:end_partition} AND (query_map['page_type']='Thank%20You' or query_map['page_type']='Thank%20You_EO')) base;
