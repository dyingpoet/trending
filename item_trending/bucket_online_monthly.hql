SET hive.EXEC.compress.output=false;

--ADD FILE /home/zzhao3/SiteP13N/Dev/zzhao3/Sams_club_bucket/time_decay.py;
ADD FILE ./time_decay.py;

USE jli21;

--set online_wt=100;
--set decay_daily=0.025;
--set prev_dt=2016-06-14;
--set last_dt=2016-07-14;


--DROP TABLE IF EXISTS temp_prev;
--CREATE TABLE temp_prev AS
--       SELECT system_item_nbr,
--       	      sum(day_sum_qty_1) AS day_sum_qty,
--	      sum(day_sum_retail_1) AS day_sum_retail,
--	      sum(day_sum_visit_1) AS day_sum_visit,
--	      visit_date                   
--        FROM        (
--		SELECT     system_item_nbr,
--                            sum(unit_qty)              AS day_sum_qty_1,
--                            sum(retail_all)            AS day_sum_retail_1,
--                            COUNT(DISTINCT visit_nbr, club_nbr, visit_date) AS day_sum_visit_1,
--                            visit_date
--                            FROM   (
--                                       SELECT visit_nbr,
--                                              club_nbr,
--                                              system_item_nbr,
--                                              unit_qty,
--                                              retail_all,
--                                              visit_date
--                                       FROM   sams_us_clubs.customer_club_day_item_sales
--                                       WHERE  visit_date>='2016-04-15'
--                                        AND   visit_date<='2016-05-14'
--						) a1
--		 GROUP BY   system_item_nbr,  visit_date 
--             UNION ALL
--		 SELECT     system_item_nbr,
--                            ${hiveconf:online_wt}*sum(unit_qty)              AS day_sum_qty_1,
--                            ${hiveconf:online_wt}*sum(retail_all)            AS day_sum_retail_1,
--                            ${hiveconf:online_wt}*COUNT(DISTINCT visit_nbr, visit_date) AS day_sum_visit_1,
--                            visit_date
--                            FROM   (	
--
--			    	       SELECT order_nbr AS visit_nbr,
--                                              system_item_nbr,
--                                              ordered_qty AS unit_qty,
--                                              ordered_qty * unit_retail_amt AS retail_all,
--                                              order_date AS visit_date
--                                        FROM  sams_us_dotcom.wc_dotcom_memeber_day_item_sales_auth
--                                        WHERE order_date>='2016-04-15' 
--                                        AND   order_date<='2016-05-14'
--                                                ) a2
--                 GROUP BY   system_item_nbr,  visit_date
--		 ) aa
--	  GROUP BY system_item_nbr,  visit_date;
-- 
--DROP TABLE if EXISTS temp_now;
--CREATE TABLE temp_now AS
--       SELECT system_item_nbr,
--		sum(day_sum_qty_1) AS day_sum_qty,
--	        sum(day_sum_retail_1) AS day_sum_retail,
--		sum(day_sum_visit_1) AS day_sum_visit,
--		visit_date
--        FROM        (
--                SELECT     system_item_nbr,
--                            sum(unit_qty)              AS day_sum_qty_1,
--                            sum(retail_all)            AS day_sum_retail_1,
--                            COUNT(DISTINCT visit_nbr, club_nbr, visit_date) AS day_sum_visit_1,
--                            visit_date
--                            FROM   (
--                                       SELECT visit_nbr,
--                                              club_nbr,
--                                              system_item_nbr,
--                                              unit_qty,
--                                              retail_all,
--                                              visit_date
--                                       FROM   sams_us_clubs.customer_club_day_item_sales
--                                       WHERE  visit_date>='${hiveconf:prev_dt}'
--                                        AND   visit_date<='${hiveconf:last_dt}'
--                                                ) a1
--                 GROUP BY   system_item_nbr,  visit_date
--             UNION ALL
--                 SELECT     system_item_nbr,
--                            ${hiveconf:online_wt} * sum(unit_qty)              AS day_sum_qty_1,
--                            ${hiveconf:online_wt} * sum(retail_all)            AS day_sum_retail_1,
--                            ${hiveconf:online_wt} * COUNT(DISTINCT visit_nbr, visit_date) AS day_sum_visit_1,
--                            visit_date
--                            FROM   (
--
--                                       SELECT order_nbr AS visit_nbr,
--                                              system_item_nbr,
--                                              ordered_qty AS unit_qty,
--                                              ordered_qty * unit_retail_amt AS retail_all,
--                                              order_date AS visit_date
--                                        FROM  sams_us_dotcom.wc_dotcom_memeber_day_item_sales_auth
--                                        WHERE order_date>='${hiveconf:prev_dt}'
--                                        AND   order_date<='${hiveconf:last_dt}'
--                                                ) a2
--                 GROUP BY   system_item_nbr,  visit_date
--                 ) aa
--         GROUP BY system_item_nbr,  visit_date;


DROP TABLE if exists sams_us_dotcom_bucket_online_monthly;
CREATE TABLE sams_us_dotcom_bucket_online_monthly AS
      SELECT * 
      FROM
          ( 
           SELECT bu.bucket, bu_map.cat_child, xx.system_item_nbr, xx.catalog_item_id, xx.sum_qty, xx.sum_decay_qty, xx.sum_retail,
                  xx.sum_decay_retail, xx.sum_visit, xx.sum_decay_visit,
		  COALESCE( yy.sum_qty_prev, 0) AS sum_qty_prev,
                  COALESCE( yy.sum_visit_prev, 0) AS sum_visit_prev,
                  COALESCE( yy.sum_retail_prev, 0) AS sum_retail_prev
		  --COALESCE( nn.sum_qty_test, 0) AS sum_qty_test,
		  --COALESCE( nn.sum_visit_test, 0) AS sum_visit_test,
		  --COALESCE( nn.sum_retail_test, 0) AS sum_retail_test,
		  --impression.impression, click.click

	   FROM  
             (  SELECT system_item_nbr, catalog_item_id, SUM(day_sum_qty) as sum_qty, SUM(decay_qty) as sum_decay_qty, SUM(day_sum_retail) as sum_retail,
	        SUM(decay_retail) as sum_decay_retail, SUM(day_sum_visit) as sum_visit, SUM(decay_visit) as sum_decay_visit
                FROM (
                     SELECT CAST(system_item_nbr as BIGINT) as system_item_nbr, catalog_item_id, CAST(day_sum_qty as DOUBLE) as day_sum_qty, CAST (decay_qty as DOUBLE) as decay_qty,
	             CAST (day_sum_retail as DOUBLE) as day_sum_retail, CAST (decay_retail as DOUBLE) as decay_retail, CAST (day_sum_visit as DOUBLE) as day_sum_visit,
	             CAST (decay_visit as DOUBLE) as decay_visit, visit_date
	             FROM (	
                          SELECT TRANSFORM (system_item_nbr, catalog_item_id, day_sum_qty, day_sum_retail, day_sum_visit, visit_date, now_date, lmda1, lmda2, lmda3) 
		          USING 'python time_decay.py' as (system_item_nbr, catalog_item_id, day_sum_qty, decay_qty, day_sum_retail, decay_retail, day_sum_visit, decay_visit, visit_date)
		          FROM  	    (
				    SELECT     x.system_item_nbr,
                                               x.catalog_item_id,
					       COALESCE(a.day_sum_qty, 0) AS day_sum_qty,
					       COALESCE(a.day_sum_retail, 0) AS day_sum_retail,
					       COALESCE(a.day_sum_visit, 0) AS day_sum_visit,
					       COALESCE(a.visit_date, '${hiveconf:prev_dt}') AS visit_date,
					       '${hiveconf:prev_dt}' as now_date,
					       '${hiveconf:decay_daily}' as lmda1,
					       '${hiveconf:decay_daily}' as lmda2,
					       '${hiveconf:decay_daily}' as lmda3
                                      FROM     
				      	       (SELECT a1.system_item_nbr     AS system_item_nbr, 
       					       a1.catalog_item_id     AS catalog_item_id, 
       					       a1.source_last_updated AS source_last_updated 
					       FROM   ( 
                			       SELECT   system_item_nbr, 
                         		       source_last_updated, 
                         		       Collect_set(catalog_item_id)[0] as catalog_item_id 
                			       FROM     sams_us_dotcom.item_catalog_xref 
                			       GROUP BY system_item_nbr, 
                         		       source_last_updated ) A1 
					       JOIN 
       					       	    ( 
                				    SELECT   system_item_nbr, 
                         			    Max(source_last_updated) AS max_source_last_updated 
                				    FROM     sams_us_dotcom.item_catalog_xref 
                				    GROUP BY system_item_nbr) B1 
						    ON     (
              					    a1.system_item_nbr = b1.system_item_nbr 
       						    AND    a1.source_last_updated = b1.max_source_last_updated)
						)x    
					LEFT JOIN							    
				      	       temp_now a
					ON (x.system_item_nbr = a.system_item_nbr)

					)before_decay
	  	 	   )after_decay
	             )format
	         GROUP BY system_item_nbr, catalog_item_id
		 )xx		 
		 
		  LEFT JOIN (
                    SELECT   system_item_nbr,
                             SUM (day_sum_visit) AS sum_visit_prev,
                             SUM (day_sum_qty) AS sum_qty_prev,
                             SUM (day_sum_retail) AS sum_retail_prev
                             FROM temp_prev
                             GROUP BY system_item_nbr
                             ) yy
               ON (xx.system_item_nbr = yy.system_item_nbr)
		 		  
	       JOIN
                   (SELECT catalog_item_id, bucket FROM jli21.sams_dotcom_item_cat_bucket WHERE ds ='${hiveconf:last_dt}' )bu
	       ON (xx.catalog_item_id = bu.catalog_item_id)

	       LEFT JOIN		      
                   (SELECT catalog_item_id, cat_parent, cat_child FROM jli21.sams_dotcom_item_cat_map WHERE ds ='${hiveconf:last_dt}' )bu_map
               ON (xx.catalog_item_id = bu_map.catalog_item_id and bu.bucket = bu_map.cat_parent)

	 )f1	      	                 
	       CROSS JOIN (
                    SELECT   SUM (day_sum_visit) AS sum_sum_visit_prev
                             FROM temp_prev
                                                            )prev_peroid              	             

	       CROSS JOIN (
		     SELECT  SUM (day_sum_visit) AS sum_sum_visit
                             FROM temp_now
							    )recent_peroid
;                                       
					
