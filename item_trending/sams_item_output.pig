REGISTER /opt/mapr/pig/pig-0.14/contrib/piggybank/java/piggybank.jar


SET DEFAULT_PARALLEL 400;
SET mapred.output.compress 'true';
SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';

DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix(); 


Items = LOAD '$INPUT' USING PigStorage('\u0001') AS (
    bucket: chararray, 
    catalog_item_id: chararray, 
    cat_child: chararray, 
    score: float
);

S0 = FOREACH Items GENERATE *, 1.0 AS score_child;

S1 = GROUP S0 BY (bucket, catalog_item_id, score);

S2 = FOREACH S1 GENERATE FLATTEN(group) AS (categoryID, nodeID, nodeWeight), S0.(cat_child, score_child) AS categoryAffinitys;

S3 = GROUP S2 BY categoryID;

S4 = FOREACH S3 GENERATE '0000' AS clubID, group AS categoryID, S2.(nodeID, nodeWeight, categoryAffinitys) AS nodeFeatureAttributes;

STORE S4 INTO '$OUTPUT' USING JsonStorage();


