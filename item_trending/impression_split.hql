
--use zzhao3;

CREATE TABLE IF NOT EXISTS impression_split (
vid string,
cid string,
sams_hubble_session string,
req_start_time string,
evar_19 string,
uagent string,
rank int,
athcpid string
)
PARTITIONED BY (dt string, dthour string)
ROW FORMAT DELIMITED STORED AS ORC;

ALTER TABLE impression_1_split drop if exists partition (dt=${hiveconf:dt}, dthour=${hiveconf:dthour});

INSERT into Table impression_1_split partition (dt=${hiveconf:dt}, dthour=${hiveconf:dthour})
SELECT vid, cid, sams_hubble_session, req_start_time, evar_19, uagent, 1, split(athcpid,'_')[0] FROM impression_1 where dt=${hiveconf:dt} and dthour=${hiveconf:dthour};

INSERT into Table impression_1_split partition (dt=${hiveconf:dt}, dthour=${hiveconf:dthour})
SELECT vid, cid, sams_hubble_session, req_start_time, evar_19, uagent, 2, split(athcpid,'_')[1] FROM impression_1 where dt=${hiveconf:dt} and dthour=${hiveconf:dthour};

INSERT into Table impression_1_split partition (dt=${hiveconf:dt}, dthour=${hiveconf:dthour})
SELECT vid, cid, sams_hubble_session, req_start_time, evar_19, uagent, 3, split(athcpid,'_')[2] FROM impression_1 where dt=${hiveconf:dt} and dthour=${hiveconf:dthour};

INSERT into Table impression_1_split partition (dt=${hiveconf:dt}, dthour=${hiveconf:dthour})
SELECT vid, cid, sams_hubble_session, req_start_time, evar_19, uagent, 4, split(athcpid,'_')[3] FROM impression_1 where dt=${hiveconf:dt} and dthour=${hiveconf:dthour};


       	    
