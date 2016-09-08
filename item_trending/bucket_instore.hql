SET hive.EXEC.compress.output=false;

ADD FILE /home/zzhao3/SiteP13N/Dev/zzhao3/Sams_club_bucket/time_decay.py;

DROP TABLE if exists zzhao3.sams_us_dotcom_bucket_store1;
CREATE TABLE zzhao3.sams_us_dotcom_bucket_store1 AS
      SELECT * 
      FROM
          ( 
           SELECT bu.bucket, xx.system_item_nbr, xx.catalog_item_id, xx.sum_qty, xx.sum_decay_qty, xx.sum_retail,
                  xx.sum_decay_retail, xx.sum_visit, xx.sum_decay_visit, yy.sum_qty_prev, yy.sum_retail_prev, yy.sum_visit_prev
	   FROM  
             (  SELECT system_item_nbr, catalog_item_id, SUM(day_sum_qty) as sum_qty, SUM(decay_qty) as sum_decay_qty, SUM(day_sum_retail) as sum_retail,
	        SUM(decay_retail) as sum_decay_retail, SUM(day_sum_visit) as sum_visit, SUM(decay_visit) as sum_decay_visit
                FROM (
                     SELECT CAST(system_item_nbr as BIGINT) as system_item_nbr, catalog_item_id, CAST(day_sum_qty as DOUBLE) as day_sum_qty, CAST (decay_qty as DOUBLE) as decay_qty,
	             CAST (day_sum_retail as DOUBLE) as day_sum_retail, CAST (decay_retail as DOUBLE) as decay_retail, CAST (day_sum_visit as BIGINT) as day_sum_visit,
	             CAST (decay_visit as BIGINT) as decay_visit, visit_date
	             FROM (	
                          SELECT TRANSFORM (system_item_nbr, catalog_item_id, day_sum_qty, day_sum_retail, day_sum_visit, visit_date, now_date, lmda1, lmda2, lmda3) 
		          USING 'python time_decay.py' as (system_item_nbr, catalog_item_id, day_sum_qty, decay_qty, day_sum_retail, decay_retail, day_sum_visit, decay_visit, visit_date)
		          FROM  	    (
				    SELECT     a.system_item_nbr,
                                               x.catalog_item_id,
                                               sum(unit_qty)              AS day_sum_qty,
                                               sum(retail_all)            AS day_sum_retail,
                                               COUNT(DISTINCT visit_nbr, club_nbr, visit_date) AS day_sum_visit,
					       a.visit_date,
					       '2016-07-04' as now_date,
					       '0' as lmda1,
					       '0' as lmda2,
					       '0' as lmda3
                                      FROM     (
                                                      SELECT visit_nbr,
                                                             club_nbr,
                                                             system_item_nbr,
                                                             unit_qty,
                                                             retail_all,
                                                             visit_date
                                                      FROM   sams_us_clubs.customer_club_day_item_sales
                                                      WHERE  visit_date>='2016-06-08'
                                                      AND    visit_date<='2016-06-14') a
                                      JOIN
                                               (
                                                       SELECT A1.system_item_nbr AS system_item_nbr,
                                                              A1.catalog_item_id AS catalog_item_id,
                                                              A1.source_last_updated AS source_last_updated FROM
                                                                   (SELECT   system_item_nbr,
                                                                             source_last_updated,
                                                                             collect_set(catalog_item_id)[0] AS catalog_item_id
                                                                      FROM   sams_us_dotcom.item_catalog_xref
                                                                      GROUP BY system_item_nbr, source_last_updated
                                                                ) A1
                                                              JOIN
                                                                   (SELECT   system_item_nbr, MAX(source_last_updated) AS max_source_last_updated
                                                                    FROM     sams_us_dotcom.item_catalog_xref
                                                                    GROUP BY system_item_nbr) B1
                                                              ON
                                                                 (A1.system_item_nbr = B1.system_item_nbr AND A1.source_last_updated = B1.max_source_last_updated)
                                                 )x
                                        ON       (
                                                        a.system_item_nbr=x.system_item_nbr)
                                        GROUP BY a.system_item_nbr, x.catalog_item_id, a.visit_date
					)before_decay
	  	 	   )after_decay
	             )format
	         GROUP BY system_item_nbr, catalog_item_id)xx

              JOIN

	   	    		   (  
				    SELECT     a1.system_item_nbr,
                                               sum(unit_qty)              AS sum_qty_prev,
                                               sum(retail_all)            AS sum_retail_prev,
                                               COUNT(DISTINCT visit_nbr, club_nbr, visit_date) AS sum_visit_prev
				     FROM     (
                                                      SELECT visit_nbr,
                                                             club_nbr,
                                                             system_item_nbr,
                                                             unit_qty,
                                                             retail_all,
                                                             visit_date
                                                      FROM   sams_us_clubs.customer_club_day_item_sales
                                                      WHERE  visit_date>='2016-06-01'
						      AND    visit_date<='2016-06-07') a1
                                        GROUP BY a1.system_item_nbr
				)yy				
               ON (xx.system_item_nbr = yy.system_item_nbr)
	       
	       JOIN
                   (SELECT catalog_item_id, bucket FROM jli21.sams_dotcom_item_cat_bucket WHERE ds ='2016-07-04' )bu
	       ON (xx.catalog_item_id = bu.catalog_item_id)
 
	       )f1
	       
	       CROSS JOIN (
							   SELECT  COUNT (DISTINCT visit_nbr, club_nbr) as sum_sum_visit
                                                           FROM   sams_us_clubs.customer_club_day_item_sales
                                                           WHERE  visit_date>='2016-06-08'
                                                           AND    visit_date<='2016-06-14'
                                                            )recent_peroid
 	       CROSS JOIN (
                                                           SELECT  COUNT (DISTINCT visit_nbr, club_nbr) as sum_sum_visit_prev
                                                           FROM   sams_us_clubs.customer_club_day_item_sales
                                                           WHERE  visit_date>='2016-06-01'
                                                           AND    visit_date<='2016-06-07'
                                                            )prev_peroid

 							  


		
;                                       
					