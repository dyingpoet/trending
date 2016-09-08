--use zzhao3;

DROP TABLE IF EXISTS samsdotcom_join;

CREATE TABLE IF NOT EXISTS samsdotcom_join(   
vid string, sams_hubble_session string, req_start_time string, a_cid string, b_cid string, c_cid string, a_rank int, b_num int, a_uagent string, b_uagent string, c_uagent string);

INSERT into table samsdotcom_join
SELECT a.vid, a.sams_hubble_session, a.req_start_time, a.cid, b.cid, c.cid, a.rank, b.num, a.uagent, b.uagent, c.uagent 
from (SELECT vid, cid, sams_hubble_session, req_start_time, evar_19, uagent, rank, athcpid FROM impression_split) a 
LEFT JOIN (SELECT vid, cid, sams_hubble_session, uagent, athcpid, cast (split(athxid,':')[3] as int) as num FROM click) b 
on (a.sams_hubble_session = b.sams_hubble_session and a.athcpid = b.athcpid) LEFT JOIN (SELECT vid, cid, sams_hubble_session, uagent FROM order) c
on (c.sams_hubble_session = b.sams_hubble_session);
