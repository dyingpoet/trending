USE jli21;

DROP TABLE if exists sams_us_dotcom_bucket_online_combined_rank;
CREATE TABLE sams_us_dotcom_bucket_online_combined_rank AS      
     SELECT 	
      j.bucket, a.title AS bucket_title, j.cat_child, j.system_item_nbr, j.catalog_item_id, b.title AS item_title, b.image_url, 
      j.sum_retail_test, j.trend_visit_test, j.sum_visit_test, j.impression, j.click, j.ctr_cap,
      j.rank_1, j.rank_2, j.rank_3, j.rank_4, j.rank_5,
      j.score_1, j.score_2, j.score_3, j.score_4, j.score_5
      --j.test_1, j.test_2, j.test_3              
      FROM 
                ( SELECT
	         *,
		 --row_number() OVER (partition BY bucket ORDER BY sum_retail_test DESC) AS test_1,
		 --row_number() OVER (partition BY bucket ORDER BY trend_visit_test DESC) AS test_2,
		 --row_number() OVER (partition BY bucket ORDER BY sum_visit_test DESC) AS test_3,
           	 row_number() OVER (partition BY bucket ORDER BY ctr_cap + 0*visit_norm + 0*trend_norm + 0*retail_norm  DESC) AS rank_1, 
		 	      ctr_cap + 0*visit_norm + 0*trend_norm + 0*retail_norm AS score_1,
		 row_number() OVER (partition BY bucket ORDER BY 0*ctr_cap + visit_norm + 0*trend_norm + 0*retail_norm  DESC) AS rank_2, 
		 	      0*ctr_cap + visit_norm + 0*trend_norm + 0*retail_norm AS score_2,
		 row_number() OVER (partition BY bucket ORDER BY 0*ctr_cap + 0*visit_norm + trend_norm + 0*retail_norm  DESC) AS rank_3, 
		 	      0*ctr_cap + 0*visit_norm + trend_norm + 0*retail_norm AS score_3,
		 row_number() OVER (partition BY bucket ORDER BY 0*ctr_cap + 0*visit_norm + 0*trend_norm + retail_norm  DESC) AS rank_4,
		 	      0*ctr_cap + 0*visit_norm + 0*trend_norm + retail_norm AS score_4,
		 row_number() OVER (partition BY bucket ORDER BY 0*ctr_cap + 2.8*visit_norm + 0*trend_norm + 1*retail_norm  DESC) AS rank_5,
		 	     		   	  	   	       		     0*ctr_cap + 2.8*visit_norm + 0*trend_norm + 1*retail_norm AS score_5
		 FROM
		            ( SELECT   bucket, cat_child, system_item_nbr, catalog_item_id, sum_qty, sum_retail, sum_retail_test,
                             sum_visit, sum_visit_test, impression, click, trend_visit_test,
         		     CASE WHEN ctr < 0.5 THEN ctr ELSE 0.5 END AS ctr_cap,
			     (sum_visit - sum_visit_min)/(sum_visit_max - sum_visit_min) AS visit_norm,
			     (trend_visit - trend_visit_min)/(trend_visit_max - trend_visit_min) AS trend_norm,
			     (sum_retail - sum_retail_min)/(sum_retail_max - sum_retail_min) AS retail_norm 
			     FROM 						
			       ( SELECT * FROM 
			               (SELECT
                                           bucket, cat_child, system_item_nbr, catalog_item_id, sum_qty, sum_retail, sum_retail_test,
                                     	   sum_visit, sum_visit_test, impression, click,
                                     	   sum_visit/float(sum_sum_visit)-sum_visit_prev/float(sum_sum_visit_prev) AS trend_visit,
                                     	   sum_visit_test/float(sum_sum_visit_test)-sum_visit/float(sum_sum_visit) AS trend_visit_test,
                                     	   COALESCE(click/impression, 0L) AS ctr
                                     	   FROM sams_us_dotcom_bucket_online_monthly
				         ) nn
			         CROSS JOIN 
			                   (SELECT
				     	   MAX(trend_visit) AS trend_visit_max, MIN(trend_visit) AS trend_visit_min,
				   	   MAX(sum_visit) AS sum_visit_max, MIN(sum_visit) AS sum_visit_min,
				   	   MAX(sum_retail) AS sum_retail_max, MIN(sum_retail) AS sum_retail_min 			     	
		                   		   FROM (
           	              	     		   SELECT 
          	              	     		   bucket, cat_child, system_item_nbr, catalog_item_id, sum_qty, sum_retail, sum_retail_test,
                              	     		   sum_visit, sum_visit_test, impression, click,
			      	     		   sum_visit/float(sum_sum_visit)-sum_visit_prev/float(sum_sum_visit_prev) AS trend_visit,
			      	     		   sum_visit_test/float(sum_sum_visit_test)-sum_visit/float(sum_sum_visit) AS trend_visit_test,
			      	     		   COALESCE(click/impression, 0L) AS ctr
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
	       WHERE  ds_selection_method = 'latest' and date_ds = '2016-07-14'
           )b
           ON (j.catalog_item_id = b.catalog_item_id)
       
;
