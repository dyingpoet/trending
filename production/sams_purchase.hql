set hive.exec.compress.output=false;
set hive.exec.parallel=true;
set hive.auto.convert.join=true;
set mapred.task.timeout 1800000
set hive.exec.reducers.bytes.per.reducer=81920000;
set mapreduce.job.reduces=1000;

--INSERT overwrite directory '/user/jli21/Databases/scs_model.db/scs_purchase_model' 
INSERT overwrite directory '${hiveconf:purchaseDir}'
SELECT          c.vid , 
                a.mid , 
                a.action_type , 
                a.catalog_item_id , 
                a.bucket , 
                a.visit_date 
FROM            ( 
                       SELECT a.mid , 
                              a.action_type , 
                              a.catalog_item_id , 
                              b.bucket , 
                              a.visit_date 
                       FROM   ( 
                                     SELECT concat_ws('', '101', 
                                            CASE 
                                                   WHEN a.membership_type_code = 2 THEN '42' 
                                                   WHEN a.membership_type_code = 3 THEN '34' 
                                                   ELSE '00' 
                                            END, lpad(cast(a.cardholder_nbr AS string), 2, '0'), '0', lpad(cast(a.membership_nbr AS string), 9, '0')) AS mid ,
                                            a.action_type , 
                                            b.catalog_item_id , 
                                            a.visit_date 
                                     FROM   ( 
                                                   SELECT b.membership_nbr, 
                                                          b.card_holder_nbr AS cardholder_nbr, 
                                                          a.membership_type_code, 
                                                          'buyClub' AS action_type, 
                                                          --AA.cardholder_type_code, 
                                                          --A.club_sic_sub_industry_code, 
                                                          b.store_nbr, 
                                                          b.visit_nbr, 
                                                          b.visit_date, 
                                                          --C.unit_qty, 
                                                          d.customer_item_nbr, --D.item_nbr, 
                                                          d.system_item_nbr 
                                                          --D.product_desc 
                                                   FROM   customers.sams_us_clubs_sams_membership_dim a
                                                   JOIN   customers.sams_us_clubs_sams_mbr_cardholder_dim aa
                                                   ON     ( 
                                                                 a.membership_nbr = aa.membership_nbr
                                                          AND    aa.current_ind='Y' 
                                                          AND    a.current_ind='Y') 
                                                   JOIN   sams_us_clubs.visit_member b 
                                                   ON     ( 
                                                                 aa.membership_nbr=b.membership_nbr
                                                          AND    aa.cardholder_nbr=b.card_holder_nbr
                                                          AND    b.visit_date >='${hiveconf:oneYrBackDate}' )
                                                   JOIN   sams_us_clubs.customer_club_day_item_sales c
                                                   ON     ( 
                                                                 b.store_nbr=c.club_nbr 
                                                          AND    b.visit_nbr=c.visit_nbr 
                                                          AND    b.visit_date=c.visit_date 
                                                          AND    c.system_item_nbr IS NOT NULL) 
                                                   JOIN   sams_us_clubs.item_info d 
                                                   ON     ( 
                                                                 c.system_item_nbr=d.system_item_nbr
                                                          AND    d.base_div_nbr=18 
                                                          AND    d.country_code='US') 
                                                   WHERE  c.unit_qty > 0 
                                                   AND    retail_all > 0 
                                                   AND    d.sub_category_nbr NOT IN (91, 
                                                                                     97) 
                                                   UNION ALL 
                                                   SELECT b.membership_nbr, 
                                                          b.cardholder_nbr, 
                                                          a.membership_type_code, 
                                                          'buyOnline' AS action_type, 
                                                          --AA.cardholder_type_code, 
                                                          --A.club_sic_sub_industry_code, 
                                                          CASE b.entry_type_code 
                                                                 WHEN 'ONLINE' THEN '6279' 
                                                                 WHEN 'AUCTION' THEN '4753' 
                                                                 ELSE '0' 
                                                          END          AS store_nbr, 
                                                          b.order_nbr  AS visit_nbr, 
                                                          b.order_date AS visit_date, 
                                                          --B.ordered_qty, --B.unit_qty, 
                                                          d.customer_item_nbr, --D.item_nbr, 
                                                          d.system_item_nbr 
                                                          --D.product_desc 
                                                   FROM   customers.sams_us_clubs_sams_membership_dim a
                                                   JOIN   customers.sams_us_clubs_sams_mbr_cardholder_dim aa
                                                   ON     ( 
                                                                 a.membership_nbr = aa.membership_nbr
                                                          AND    aa.current_ind='Y' 
                                                          AND    a.current_ind='Y') 
                                                   JOIN 
                                                          ( 
                                                                 SELECT * 
                                                                 FROM   sams_us_dotcom.wc_dotcom_memeber_day_item_sales_auth
                                                                 WHERE  unit_retail_amt > 0 
                                                                 AND    ordered_qty > 0) b 
                                                   ON     ( 
                                                                 aa.membership_nbr=b.membership_nbr
                                                          AND    aa.cardholder_nbr=b.cardholder_nbr
                                                          AND    b.order_date >='${hiveconf:oneYrBackDate}')
                                                   JOIN   sams_us_dotcom.item_info d 
                                                          --join  sams_us_clubs.item_info D 
                                                   ON     ( 
                                                                 b.system_item_nbr = d.system_item_nbr
                                                          AND    d.country_code='US') ) a 
                                     JOIN   jli21.sams_prod_map b 
                                     ON     a.system_item_nbr = b.system_item_nbr ) a 
                       JOIN   jli21.sams_dotcom_item_cat_bucket b 
                       ON     a.catalog_item_id = b.catalog_item_id 
                       WHERE  b.ds = '${hiveconf:yesterday}' ) a 
LEFT OUTER JOIN (SELECT vid, mid FROM jli21.sams_vid_mapping_dt WHERE dt = '${hiveconf:yesterday}') c 
ON              a.mid = c.mid 
--WHERE           c.dt = '${hiveconf:yesterday}' ;
;


