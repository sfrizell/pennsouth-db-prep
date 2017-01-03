pennsouth_db_prep
================

The pennsouth_db_prep directory contains scripts used to update MySQL database tables in preparation for running the nightly MDS to Aweber update program.

Flow of data:

(1) Nightly ftp from MDS to Rosehosting of MDS export file. File location on Rose Hosting service: /home/mds/public_ftp/mds_export.csv

(2) Run the script 'setup_db_for_pennsouth_aweber_nightly_run.sh' to:
	(a) copy the mds_export.csv from the source location noted above to the pennsouth_db_prep/data directory
	(b) call the script 'truncate_load_mds_export_pennsouth_resident.sh'
	(c) rename the mds_export.csv script in the source directory so that only newly ftp'd versions of the script will be run once the current run is complete
	