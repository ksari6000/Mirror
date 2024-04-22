#!/bin/bash

#Things to do create aliases so I can mail crap out to people

export ORACLE_HOME="/usr/lib/oracle/11.1/client"
export LD_LIBRARY_PATH="/usr/lib/oracle/11.1/client/lib"


declare -a var  #create an array
idx=0
run_dir="/usr/local/mccs/bin"
io_dir="/usr/local/mccs/data/sas/04_Runtime"
weekly_file="/usr/local/mccs/data/sas/04_Runtime/merch_weeks"
sas_file="/usr/local/mccs/data/sas/04_Runtime/sas_weekly_sw.txt"

echo ""
echo "SAS Weekly Input Files Start"
echo "Date: $(date +%Y-%m-%d)"
echo "Script Start Time: $(date +%H:%M:%S)"
echo ""
echo ""
echo "Checking arguments passed from cron/commandline to see what weeks to run"

case $1 in
  prev ) echo "We will be processing previous 5 weeks" ;;
  curr ) echo "We will be processing the last closed week. Meaning week end date of Sunday" ;;
     * ) echo "There was an error \"curr\" or \"prev\" was not privided!!!!!"
         echo "How am I to know what to process?"
         exit 2;;
esac

echo "Running SqlPlus to start SAS Data Batch Process"
/usr/bin/sqlplus -S eric/ericdata@draix12.usmc-mccs.org/sasprd @$io_dir/sas_weekly_get_$1_weeks.sql > $io_dir/merch_weeks
/usr/bin/sqlplus -S eric/ericdata@draix12.usmc-mccs.org/sasprd @$run_dir/sas_weekly_$1_start.sql
echo ""
echo ""



# Process Merch and Product files
# Process OnOrder takes approx 1 hour to 1.5 hours

echo "Creating Merch/Product"
echo ""
for a in DIVISION LOB DEPARTMENT CLASS SUBCLASS STYLE_DAILY PRODUCT_DAILY; do
  nohup  perl  /usr/local/mccs/bin/sas_data.pl --type $a --database rms_p_force  &
  var[$idx]=`echo $!`
   #echo "exe PID ".${var[$idx]}     
  idx=`expr $idx + 1`
done

echo ""
echo "Creating SALES"
echo "Lets start creating SALES files  $(date +%k:%M:%S)"
while read line; do
    rec=`echo $line | cut -d "," -f 1 | tr -d ' '`
    year=`echo $line | cut -d "," -f 2 | tr -d ' '`
    week=`echo $line | cut -d "," -f 3 | tr -d ' '`
    if [[ $rec =~ "^rec" ]] 
    then
      echo "Just kicking off another SALES process $year $week"
      nohup /usr/local/mccs/bin/sas_data.pl --type SALE_SAS_PROD --database rms_p_force --merchyear $year  --merchweek $week &
      var[$idx]=`echo $!`
      idx=`expr $idx + 1`
    else
      echo "Hmmm look like there are no more weeks to process (Garbage at end of file)"
    fi
done <"$weekly_file"


cnt=0
echo "Waiting For creation of Sas_Prod_Complete to Finish"
echo "We can be waiting for over 5 hours for task to complete... Depends on the stress that is on MC2P"
echo ""
echo "!!!!!!!!!!!!!!!!!!This will be as exciting as watching paint dry!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo ""
echo "****Please be patient, I will check every 20 minutes !!!DO NOT KILL - Check SAS MCCS DB if process is going long!!!"
echo "****An update will be echoed every hour"
echo "****Verify SAS schema to see if oracle monitor is showing data movement for wkly_sas tables"
echo ""

echo "Lets Begin the wait $(date +%k:%M:%S)"
flag="false"
while [[ $flag != "true" ]]; do
  sleep 1200 #every 300 seconds = 5 minutes
  /usr/bin/sqlplus -S eric/ericdata@draix12.usmc-mccs.org/sasprd @$run_dir/sas_weekly_complete.sql > $io_dir/sas_weekly_sw.txt
  sleep 10 #
#  echo "20 Minutes have passed $(date +%k:%M:%S)"
  tmp=""
  while read line; do
    tmp=`echo $line | cut -d "," -f 2 | tr -d ' '`
    if [[ $tmp == "true" ]]
     then
     flag=$tmp
    fi
  done <"$sas_file"
  cnt=`expr $cnt + 1`
  if [[ $cnt == "3" ]]
    then
      cnt=0
      echo "Still Waiting - Time is now-> $(date +%k:%M:%S)"
  fi
done
echo ""
echo "WooHoo we are done waiting for data table to be created $(date +%k:%M:%S)"


echo ""
echo "Creating Inventory"
echo "Lets start creating INVENTORY files  $(date +%k:%M:%S)"
while read line; do
    rec=`echo $line | cut -d "," -f 1 | tr -d ' '`
    year=`echo $line | cut -d "," -f 2 | tr -d ' '`
    week=`echo $line | cut -d "," -f 3 | tr -d ' '`
    if [[ $rec =~ "^rec" ]] 
    then
      echo "Just kicking off another INVENTORY process $year $week"
      nohup /usr/local/mccs/bin/sas_data.pl --type INVENTORY_SAS_PROD --database sasprd  --merchyear "$year" --merchweek "$week" &
      var[$idx]=`echo $!`
      idx=`expr $idx + 1`
    else
      echo "Hmmm look like there are no more weeks to process (Garbage at end of file)"
    fi
done <"$weekly_file"

#run on order as late as possible
echo ""
echo "Creating OnOrder  "
echo "Lets start creating ONORDER files  $(date +%k:%M:%S)"
nohup  perl  /usr/local/mccs/bin/sas_data.pl --type ONORDER --database rms_p_force  &
idx=`expr $idx + 1`
var[$idx]=`echo $!`


echo "Lets wait for all the flat files to finish"
# Wait for the longest process to finish before continuing
for  a in "${var[@]}"; do 
  wait $a 
done
echo "WooHoo files have been created   $(date +%k:%M:%S)"

echo ""
echo "Now it is time to scrub MFINC for duplicates   $(date +%k:%M:%S)"
declare -a listing
dir="/usr/local/mccs/data/sas/"
cd $dir
listing=`ls -1 $dir | grep MFINC`
echo "Files we will be checking $listing"
for i in $listing; do
  filename=`echo $i | sed 's/001/002/'`
  filename2=`echo $i | sed 's/001/003/'`
  echo "checking file $i for duplicates"
  sort $i | uniq -d > $filename
  cnt=`wc -l $filename | cut -d " " -f1`
  if [[ $cnt == "0" ]]
  then
    rm $filename
  else
   echo "Duplicate found in $i"
   echo "Created distinct file of duplicates now known as $filename"
   echo "New file with no duplicates is now known as $filename2"
   sort $i | uniq -u > $filename2
   rm $i
   echo "Removing original file $i"
  fi
  echo ""
done
echo "Oh boy! we are done with the scrubbing   $(date +%k:%M:%S) Lets continue"


echo "Not running SFTP Now"
#echo ""
#echo "Here is where we ftp files to sasmdi draix12"
# use expect to send files over sftp using commandline batch
#/usr/local/mccs/bin/sas_weekly_sftp.exp
#b=`echo $!`
#wait $b

echo ""
echo "Since the files have been ftp'd lets archive them"
#Archive info after files have been ftp'd
directory="/usr/local/mccs/data/sas/03_Weekly/$(date +%Y%m%d)_weekly"
echo "Archiving to $directory"

mkdir -p $directory
mv /usr/local/mccs/data/sas/MERCH* $directory/.
mv /usr/local/mccs/data/sas/MINV*  $directory/.
mv /usr/local/mccs/data/sas/MFINC* $directory/.
mv /usr/local/mccs/data/sas/MON*   $directory/.

echo "SAS Weekly Input Complete"
echo "Completeion Time: $(date +%H:%M:%S)"
echo ""
