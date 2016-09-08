DROP TABLE if EXISTS zzhao3.user_bucket_raw;
CREATE TABLE zzhao3.user_bucket_raw AS 

SELECT vid, mid, action_type,  catalog_item_id,  bucket,  session_date_val AS visit_date FROM
(
SELECT          a.vid, 
                c.mid, 
                a.action_type, 
                a.catalog_item_id, 
                a.bucket, 
                a.session_date_val 
FROM            ( 
                       SELECT a.vid, 
                              a.action_type, 
                              b.catalog_item_id AS catalog_item_id, 
                              b.bucket, 
                              a.session_date_val 
                       FROM   ( 
                                     SELECT vid, 
                                            action_type, 
                                            item_ids, 
                                            session_date_val 
                                     FROM   pythia.samsdotcom_hubble_item_action_raw lateral VIEW explode(visitor_id) vid_table AS vid
                                     WHERE  session_date>="20160314"  and session_date<="20160614"
                                     AND    action_type IN ('add_to_cart', 'order',  'item_view') ) a 
                       JOIN 
                              ( 
                                     SELECT * 
                                     FROM   jli21.sams_dotcom_item_cat_bucket 
                                     WHERE  ds = '2016-07-14') b 
                       ON     a.item_ids = b.catalog_item_id 
                ) a 
LEFT OUTER JOIN 
                (SELECT vid, mid FROM jli21.sams_vid_mapping_dt WHERE dt = '2016-07-14') c 
ON              a.vid = c.vid )d 
 where mid IS NOT NULL AND vid is NOT NULL AND bucket is NOT NULL 
;

DROP TABLE IF EXISTS zzhao3.user_bucket_counted;
CREATE TABLE zzhao3.user_bucket_counted AS 

SELECT mid, bucket, SUM(score) AS score
FROM (
     SELECT mid, bucket, COUNT (*) AS score FROM zzhao3.user_bucket_raw 
     where action_type == 'item_view' GROUP BY mid, bucket 
     UNION ALL
     SELECT mid, bucket, 5 * COUNT (*) AS score FROM zzhao3.user_bucket_raw
     where action_type == 'add_to_cart' GROUP BY mid, bucket
     UNION ALL
     SELECT mid, bucket, 10 * COUNT (*) AS score FROM zzhao3.user_bucket_raw
     where action_type == 'order' GROUP BY mid, bucket
     )a
GROUP BY mid, bucket
;


DROP TABLE if exists zzhao3.user_bucket_process;

CREATE TABLE zzhao3.user_bucket_process AS

SELECT mid, bucket, SUM (factor) AS factor
FROM(

        SELECT aa.mid, bb.bucket, 1.0 AS factor
               FROM
               (SELECT mid FROM zzhao3.user_bucket_raw GROUP BY mid)aa
               CROSS JOIN
               (SELECT bucket FROM jli21.sams_dotcom_item_cat_bucket WHERE ds ='2016-07-14' GROUP BY bucket)bb
        UNION ALL
        SELECT mid, bucket, log(4, score + 1)/2 AS factor FROM zzhao3.user_bucket_counted
)a
GROUP BY mid, bucket
;


DROP TABLE IF EXISTS zzhao3.user_bucket_item;

CREATE TABLE zzhao3.user_bucket_item AS

SELECT * FROM
       (
        SELECT aa.mid, aa.bucket, aa.cat_child, aa.catalog_item_id, aa.system_item_nbr, aa.score,
        row_number() OVER (partition BY aa.mid, aa.bucket ORDER BY aa.score  DESC) AS rank
                     FROM
                     (SELECT a.mid, a.bucket, b. cat_child, b.catalog_item_id, b.system_item_nbr, a.factor * b.score_5 AS score
                             FROM
                             (SELECT mid, bucket,factor FROM user_bucket_process)a

                             CROSS JOIN

                             (SELECT
                             bucket, cat_child, system_item_nbr, catalog_item_id, item_title,rank_5, score_5
                             FROM sams_us_dotcom_bucket_online_combined_rank where rank_5 <= 30)b
                             where a.bucket = b.cat_child
                             )aa
                      )aaa
where rank <= 30;
