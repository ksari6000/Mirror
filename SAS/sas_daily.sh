#!/bin/bash
################################################################################
# SAS DAILY BATCH SCRIPT
# Purpose:  Create daily file for SAS input and to archive files after Sftp
# Updated by Kaveh Sari, 4/17/2024
# Added sas directory to /usr/local/data/mccs on 0010 as well as the 02_Daily &
# 04_BatchScripts directories.
#
#
################################################################################
declare -a var
idx=0

echo "SAS Daily Input Files "
echo "Date: $(date +%Y-%m-%d)"
echo "Start Time: $(date +%H:%M:%S)"
echo "Creating Merch/Product"

for a in DIVISION LOB DEPARTMENT CLASS SUBCLASS STYLE_BOP PRODUCT_BOP; do
  nohup  perl  /usr/local/mccs/bin/sas_data.pl --type $a --database rms_p_force  &
  var[$idx]=`echo $!`
  idx=`expr $idx + 1`
done

for  a in "${var[@]}"; do
  wait $a
done

echo "Merch/Product Created"

echo "Not running SFTP Now"
#echo "SFTP of Merch/Product"
#/usr/local/mccs/data/sas/04_BatchScripts/sas_daily_sftp.exp
#b=`echo $!`
#$wait $b

echo "Archiving to 02_Daily Directory"
directory="/usr/local/mccs/data/sas/02_Daily/$(date +%Y%m%d)_daily"
mkdir -p $directory
mv /usr/local/mccs/data/sas/MERCH*   $directory/.

echo "SAS Daily Input Complete"
echo "Completeion Time: $(date +%H:%M:%S)"
echo ""


