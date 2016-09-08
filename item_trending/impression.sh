xx=$(date +"%Y-%m-%d %H:00" -d "2016-06-01 00:00")

for d in {0..26}
do
    for h in {0..23}
    do
       dt1=$(date +"%Y-%m-%d %H:00" -d "$xx $d days $h hours")
       dt2=$(date +"%Y-%m-%d %H:00" -d "$xx $d days $h hours 1 hour")
       dt1p=$(date +"%Y-%m-%d-%H-00" -d "$dt1");
       dt2p=$(date +"%Y-%m-%d-%H-00" -d "$dt2");
      dt=$(date +"%Y-%m-%d" -d "$dt1");
      s=$(($(date --date="$dt1" +%s)/360))
      e=$(($(date --date="$dt2" +%s)/360))

    cmd="/usr/local/bin/hive  --hiveconf start_partition=$s --hiveconf end_partition=$e --hiveconf dthour=\'$dt1p\' --hiveconf dt=\'$dt\' -f /home/zzhao3/2016_06_Trending/Sam_club_trending/from_Wei/impression_2.hql"

     echo $cmd
     eval $cmd

    done
done


