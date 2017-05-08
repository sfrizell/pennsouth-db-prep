#!/bin/bash
#  script: setup_db_for_pennsouth_aweber_nightly_run.sh 
#  author: s. frizell
#  date: 12/12/2016
#  function:  
#	(1) Copy the mds_export.csv file from the mds home directory into the 
#			current directory.
#	(2) Run the truncate/load/insert script to populate the following MySQL
#		tables:
#			- MDS_EXPORT
#			- PENNSOUTH_RESIDENT
#	(3) Write the success/failure status of the run to the file: /home/pennsouthdata/pennsouth_aweber/status.txt
#	(4) The Mds/Aweber update program can check the status of the job as recorded in step 
#		(3) above and exit if this load has failed. 
#
#   - Invoke this script via following command to pipe output to a log file:
#         ./setup_db_for_pennsouth_aweber_nightly_run.sh > output_truncate_load.log

MDS_FTP_DIRECTORY=/home/mds/public_ftp
MDS_EXPORT_FILE_SOURCE=${MDS_FTP_DIRECTORY}/mds_export.csv
MDS_EXPORT_ARCHIVED_FILENAME=mds_export.csv.archived
MDS_EXPORT_ARCHIVED=${MDS_FTP_DIRECTORY}/${MDS_EXPORT_ARCHIVED_FILENAME}
MDS_AWEBER_ROOT_DIR=/home/pennsouthdata/pennsouth_db_prep
MDS_EXPORT_TARGET_DEST=${MDS_AWEBER_ROOT_DIR}/data/mds_export.csv 
RUN_AWEBER_LIST_MGMT_REPORTS=invoke_mds_aweber_list_mgmt_rpt.sh
DB_STATUS_FILE="./../pennsouth_aweber/status.txt"


# Check for existence of file; if it exists copy it 
DATE=`date +%Y-%m-%d:%H:%M:%S`
if [ -f $MDS_EXPORT_FILE_SOURCE ]
then
	cp $MDS_EXPORT_FILE_SOURCE $MDS_EXPORT_TARGET_DEST
	status=$?
	if [ "$status" -eq 0 ] 
	then
		./truncate_load_mds_export_pennsouth_resident.sh  # execute the shell script to truncate and load mds_export / pennsouth_resident 
		status=$?
		if [ "$status" -eq 0 ]
		then
			mv $MDS_EXPORT_FILE_SOURCE $MDS_EXPORT_ARCHIVED   # so we don't process the same file again on the next run...
			echo "truncate load script processed successfully."
			echo "db_update_status=success		$DATE" > $DB_STATUS_FILE 
			exit 0
		else
			echo "truncate load script failed!"
			echo "db_update_status=failure	$DATE" > $DB_STATUS_FILE
			exit 1
		fi 
	else
		echo "cp of mds_export source to mds_export target failed."
		echo "db_update_status=failure	cp of mds_export source to target failed.	$DATE" > $DB_STATUS_FILE
		exit 1
	fi 
else
	echo "db_update_status=failure	mds_export source file not found."
	echo "db_update_status=failure	mds_export source file not found.	$DATE" > $DB_STATUS_FILE
	exit 1
fi 
