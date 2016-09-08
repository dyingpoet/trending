USE jli21;

DROP TABLE if exists sams_us_dotcom_bucket_online_combined_rank;
CREATE TABLE sams_us_dotcom_bucket_online_combined_rank AS      
     SELECT 	
      j.bucket, a.title AS bucket_title, j.cat_child, j.system_item_nbr, j.catalog_item_id, b.title AS item_title, b.image_url, 
      j.rank_5, j.score_5
      FROM 
                ( SELECT
	         *,
		 row_number() OVER (partition BY bucket ORDER BY ${hiveconf:visit_wt} * visit_norm + ${hiveconf:trend_wt} * trend_norm + 1*retail_norm  DESC) AS rank_5,
		 	     		   	  	   	       		     ${hiveconf:visit_wt} * visit_norm + ${hiveconf:trend_wt} * trend_norm + 1*retail_norm AS score_5
		 FROM
		             (SELECT   bucket, cat_child, system_item_nbr, catalog_item_id, sum_qty, sum_retail,
                             sum_visit,
         		     --CASE WHEN ctr < 0.5 THEN ctr ELSE 0.5 END AS ctr_cap,
			     (sum_visit - sum_visit_min)/(sum_visit_max - sum_visit_min) AS visit_norm,
			     (trend_visit - trend_visit_min)/(trend_visit_max - trend_visit_min) AS trend_norm,
			     (sum_retail - sum_retail_min)/(sum_retail_max - sum_retail_min) AS retail_norm 
			     FROM 						
			       ( SELECT * FROM 
			               (SELECT
                                           bucket, cat_child, system_item_nbr, catalog_item_id, sum_qty, sum_retail, 
                                     	   sum_visit,
                                     	   sum_visit/float(sum_sum_visit)-sum_visit_prev/float(sum_sum_visit_prev) AS trend_visit
                                     	   FROM sams_us_dotcom_bucket_online_monthly
				         ) nn
			         CROSS JOIN 
			                   (SELECT
				     	   MAX(trend_visit) AS trend_visit_max, MIN(trend_visit) AS trend_visit_min,
				   	   MAX(sum_visit) AS sum_visit_max, MIN(sum_visit) AS sum_visit_min,
				   	   MAX(sum_retail) AS sum_retail_max, MIN(sum_retail) AS sum_retail_min 			     	
		                   		   FROM (
           	              	     		   SELECT 
          	              	     		   bucket, cat_child, system_item_nbr, catalog_item_id, sum_qty, sum_retail,
                              	     		   sum_visit,
			      	     		   sum_visit/float(sum_sum_visit)-sum_visit_prev/float(sum_sum_visit_prev) AS trend_visit
			      	     		   --COALESCE(click/impression, 0L) AS ctr
          		     	     		   FROM sams_us_dotcom_bucket_online_monthly
             		     	     		   ) g
			                     )mm
 				  )combined
			 )be   
	             )j	     

           LEFT JOIN 
           pythia.samsdotcom_catalog_node_name a
           ON (j.bucket = a.leaf)
      
           LEFT JOIN
           (
              SELECT  catalog_item_id, title, image_url FROM pythia.sams_us_dotcom_item_catalog_daily_table_with_inactive_items
	       WHERE  ds_selection_method = 'latest' and date_ds = '${hiveconf:last_dt}'
           )b
           ON (j.catalog_item_id = b.catalog_item_id)
;
