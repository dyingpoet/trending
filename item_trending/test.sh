

#hive -f impression.hql -hiveconf dt= -hiveconf dthour= -hiveconf start_partition= -hiveconf end_partition=

hive -f bucket_online_monthly.hql -hiveconf online_wt=100 -hiveconf decay_daily=0.025 -hiveconf prev_dt=2016-06-14 -hiveconf last_dt=2016-07-14


