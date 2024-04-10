#!/bin/bash
#Comments 
export ORACLE_HOME="/usr/lib/oracle/11.1/client64"
export LD_LIBRARY_PATH="/usr/lib/oracle/11.1/client64/lib"

declare -a var  #create an array
idx=0
weekly_file="/usr/local/mccs/data/sps/work/weeks.dat"

echo "Lets start creating SPS files  $(date +%k:%M:%S)"

#/usr/bin/sqlplus -S rdiusr/stop50@draix01.usmc-mccs.org/mc2p @/usr/local/mccs/bin/sps_get_weeks.sql > $weekly_file

sqlplus -S rdiusr/stop50@draix01.usmc-mccs.org/mc2p @/usr/local/mccs/bin/sps_get_weeks.sql > $weekly_file

nohup perl /usr/local/mccs/bin/sps_data_load.pl --type LOCATION --database rms_r --archive --nosend&
var[$idx]=`echo $!`
idx=`expr $idx + 1`

nohup perl /usr/local/mccs/bin/sps_data_load.pl --type PRODUCT --database rms_r  --archive --nosend&
var[$idx]=`echo $!`
idx=`expr $idx + 1`

echo ""
echo "Processing SPS activity file"
while read line; do
    rec=`echo $line | cut -d "," -f 1 | tr -d ' '`
    vdate=`echo $line | cut -d "," -f 3 | tr -d ' '`
    if [[ $rec = "rec" ]] 
    then
      echo "Just kicking off another SPS Activity process $vdate"
      nohup perl /usr/local/mccs/bin/sps_data_load.pl --type ACTIVITY --database rms_p_force --week_ending "$vdate" --archive &
      var[$idx]=`echo $!`
      idx=`expr $idx + 1`
    else
      echo "Hmmm look like there are no more weeks to process (Garbage at end of file)"
    fi
done <"$weekly_file"


echo "Lets wait for all the flat files to finish"
## Wait for the longest process to finish before continuing
for  a in "${var[@]}"; do
  wait $a
done
echo "WooHoo files have been created   $(date +%k:%M:%S)"
