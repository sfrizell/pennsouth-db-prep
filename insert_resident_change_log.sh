#!/bin/bash

#  script: insert_resident_change_log.sh
#  author: s. frizell
#  date: 06/15/2017
#  function: insert detected changes in MDS_Export file from last run to this run into the resident_change_log table.
#  NOTE: This script should be run after the mds_export and pennsouth_resident table have been truncated and rebuilt
#         from the nightly MDS Export file.
#

#   prod environment:
#     mysql --defaults-file=/home/pennsouthdata/.my.cnf  -D pennsout_db -h 127.0.0.1 <<STOP
#   dev environment:
#     mysql --defaults-file=/Users/sfrizell/.my.cnf -D pennsouth_db -h 127.0.0.1 <<STOP

DATE=`date +%Y-%m-%d:%H:%M:%S`
DB_STATUS_FILE="./../pennsouth_aweber/insert_resident_change_log_status.txt"

mysql --defaults-file=/home/pennsouthdata/.my.cnf  -D pennsout_db -h 127.0.0.1 <<STOP

--  maintain audit trail of time script takes to run...
SET @start=UNIX_TIMESTAMP();


--  new email added
insert ignore into resident_change_log
(person_id, email_address, first_name, last_name, apartment_id, building,
floor_number, apt_line, mds_resident_category, insert_date, change_type)
select person_id, email_address, first_name, last_name, pennsouth_apt_apartment_id, building,
floor_number, apt_line, mds_resident_category, curdate(), 'EmailAdd'
from pennsouth_resident pr
where not exists
(select 'x'
from
	 resident_change_log cl
where  pr.email_address = cl.email_address
and cl.insert_date =
(select max(cl2.insert_date)
from resident_change_log cl2
where cl.resident_change_log_id = cl2.resident_change_log_id
)
);

-- email address removed
insert ignore into resident_change_log
(person_id, email_address, first_name, last_name, apartment_id, building,
floor_number, apt_line, mds_resident_category, insert_date, change_type)
select person_id, email_address, first_name, last_name, apartment_id, building,
floor_number, apt_line, mds_resident_category, curdate(), 'EmailRemove'
from resident_change_log cl
where
 not exists
(select 'x'
from
	 pennsouth_resident pr
where
cl.email_address = pr.email_address)
and not exists
(
select 'x'
from resident_change_log cl2,
(
select max(cl3.insert_date) max_insert_date, cl3.email_address
from resident_change_log cl3
group by cl3.email_address
) max_res_log
where cl2.insert_date = max_res_log.max_insert_date
and cl2.email_address = max_res_log.email_address
and cl2.change_type = 'EmailRemove'
);


--  new person_id added
insert ignore into resident_change_log
(person_id, email_address, first_name, last_name, apartment_id, building,
floor_number, apt_line, mds_resident_category, insert_date, change_type)
select person_id, email_address, first_name, last_name, pennsouth_apt_apartment_id, building,
floor_number, apt_line, mds_resident_category, curdate(), 'PersonIdAdd'
from pennsouth_resident pr
where not exists
(select 'x'
from
	 resident_change_log cl
where  pr.person_id = cl.person_id
and (cl.change_type = 'PersonIdAdd' or cl.change_type = 'InitialLoad')
and cl.insert_date =
(select max(cl2.insert_date)
from resident_change_log cl2
where cl.resident_change_log_id = cl2.resident_change_log_id
)
);

-- person_id removed
insert ignore into resident_change_log
(person_id, email_address, first_name, last_name, apartment_id, building,
floor_number, apt_line, mds_resident_category, insert_date, change_type)
select person_id, email_address, first_name, last_name, apartment_id, building,
floor_number, apt_line, mds_resident_category, curdate(), 'PersonIdRemove'
from resident_change_log cl
where not exists
(select 'x'
from
	 pennsouth_resident pr
where
cl.person_id = pr.person_id)
and not exists
(
select 'x'
from resident_change_log cl2,
(
select max(cl3.insert_date) max_insert_date, cl3.person_id
from resident_change_log cl3
group by cl3.person_id
) max_res_log
where cl2.insert_date = max_res_log.max_insert_date
and cl2.person_id = max_res_log.person_id
and cl2.change_type = 'PersonIdRemove'
);

-- change to mds_resident_category
insert ignore into resident_change_log
(person_id, email_address, first_name, last_name, apartment_id, building,
floor_number, apt_line, mds_resident_category, insert_date, change_type, before_value, current_value)
select distinct pr.person_id, pr.email_address, pr.first_name, pr.last_name, pr.pennsouth_apt_apartment_id, pr.building,
pr.floor_number, pr.apt_line, pr.mds_resident_category, curdate(), 'ResidentCategory', cl.mds_resident_category, pr.mds_resident_category
from resident_change_log cl
 inner join pennsouth_resident pr
 on cl.person_id = pr.person_id
 inner join
 ( select  max(cl.insert_date) max_insert_date, cl.person_id
 from resident_change_log cl
 group by cl.person_id) max_res_change_log
 on cl.person_id = max_res_change_log.person_id
 and cl.insert_date = max_res_change_log.max_insert_date
where
 cl.mds_resident_category <> pr.mds_resident_category;


--  display statistics on runtime of script...
SET
@s=@seconds:=UNIX_TIMESTAMP()-@start,
@d=TRUNCATE(@s/86400,0), @s=MOD(@s,86400),
@h=TRUNCATE(@s/3600,0), @s=MOD(@s,3600),
@m=TRUNCATE(@s/60,0), @s=MOD(@s,60),
@day=IF(@d>0,CONCAT(@d,' day'),''),
@hour=IF(@d+@h>0,CONCAT(IF(@d>0,LPAD(@h,2,'0'),@h),' hour'),''),
@min=IF(@d+@h+@m>0,CONCAT(IF(@d+@h>0,LPAD(@m,2,'0'),@m),' min.'),''),
@sec=CONCAT(IF(@d+@h+@m>0,LPAD(@s,2,'0'),@s),' sec.');

SELECT
CONCAT(@seconds,' sec.') AS seconds,
CONCAT_WS(' ',@day,@hour,@min,@sec) AS elapsed;

STOP

table_load_status=$?
if [ "$table_load_status" -eq 0 ]
then
    echo "insert resident_change_log processed successfully."
	echo "db_update_status=success		$DATE" > $DB_STATUS_FILE
    exit 0
else
    echo "insert resident_change_log failed!"
	echo "db_update_status=failure		$DATE" > $DB_STATUS_FILE
	exit 1
fi 
