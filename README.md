pennsouth_db_prep
================

The pennsouth_db_prep directory contains scripts used to update MySQL database tables in preparation for running the nightly MDS to Aweber update program.

Flow of data:

(1) Nightly ftp from MDS to Rosehosting of MDS export file. File location on Rose Hosting service: /home/mds/public_ftp/mds_export.csv

(2) Run the script 'setup_db_for_pennsouth_aweber_nightly_run.sh' to:

* (a) copy the mds_export.csv from the source location noted above to the pennsouth_db_prep/data directory	
* (b) call the script 'truncate_load_mds_export_pennsouth_resident.sh'
* (c) rename the mds_export.csv script in the source directory so that only newly ftp'd versions of the script will be run once the current run is complete
	
	
### Description of script: truncate_load_mds_export_pennsouth_resident.sh

The shell script 'truncate_load_mds_export_pennsouth_resident.sh' does the following:

* Truncates and loads the mds_export MySQL table from the MDS Export file ftp'd nightly to the server by MDS.
* Truncates the pennsouth_resident table and uses the mds_export table as input to re-populates pennsouth_resident from mds_export. The following transformations are performed on the mds_export data when loading to pennsouth_resident:
 
    * wherever multiple email addresses are listed (separated by semi-colons) in the mds_export.email_address column for a resident, create a separate row in the pennsouth_resident table for each email_address.
    * populate the pennsouth_resident.vehicle_reg_exp_countdown column by calculating the number of days between the mds_export.vehicle_reg_exp_date (vehichle registration expiration date) and the current date. If the registration is expired, store as a negative number the days since expiration.
    * populate the pennsouth_resident.vehicle_reg_interval_remaining by setting the value to '21' if the number of days remaining before expiration is in the range of 10 days to 21 days; '10' if the number of days remaining is 0 to 10, and '0' if the expiration data has been reached or exceeded.
    * populate the pennsouth_resident.homeowners_ins_exp_countdown column by calculating the number of days between the mds_export.homeowner_insurance_exp_date and the current date. If the insurance expiration date has been reached, store as a negative number the days since expiration.
    * populate the pennsouth_resident.homeowner_ins_interval_remaining column by setting the value to '21' if the number of days remaining before expiration is in the range of 10 to 21 days; '10' if the number of days remaining is 0 to 10, and '0' if the expiration has been reached.
    