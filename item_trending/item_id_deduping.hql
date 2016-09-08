
USE jli21;
drop table if exists sams_system_item_nbr_catalog_item_id_deduping;
CREATE TABLE sams_system_item_nbr_catalog_item_id_deduping AS
SELECT A.system_item_nbr AS system_item_nbr, 
       A.catalog_item_id AS catalog_item_id,
       A.source_last_updated AS source_last_updated FROM
(SELECT system_item_nbr, 
        source_last_updated,
        collect_set(catalog_item_id)[0] AS catalog_item_id 
	FROM sams_us_dotcom.item_catalog_xref
	GROUP BY system_item_nbr, source_last_updated
) A JOIN 
(SELECT system_item_nbr, MAX(source_last_updated) AS max_source_last_updated FROM sams_us_dotcom.item_catalog_xref
GROUP BY system_item_nbr) B ON
(A.system_item_nbr = B.system_item_nbr AND A.source_last_updated = B.max_source_last_updated)
--item_id_deduped
