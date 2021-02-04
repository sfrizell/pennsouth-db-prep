#!/bin/bash

#  script: truncate_load_mds_export_pennsouth_resident.sh 
#  author: s. frizell
#  date: 12/22/2016
#  function: import the mds_export.csv file into the pennsout_db.mds_export table 
#  Steps:
#	(1) Check for presence 	
#	(1) Copy /home/mds/public_ftp/mds_export.csv to ./../data/
#   
#   (2)  strip any non-ascii characters from the mds_export.csv file -- not yet implemented! (1/5/2-17)
#   (3) truncate mds_export table and load mds_export.file to mds_export Table 
#	(4) truncate pennsouth_resident table and insert from mds_export table. 
#   Note: to grant all privileges to a user: 
#			mysql> GRANT ALL PRIVILEGES ON database_name.* TO 'username'@'localhost';
#   12/30/2016 - modified to populate new columns (a) vehicle_reg_interval_remaining
#	(b) homeowner_ins_interval_remaining
#   1/9/2017 - remove office_telephone column from mds_export and pennsouth_resident
#   4/19/2017 - Change countdown expiration interval of 11 - 21 to 11 - 24 for homeowners insurance and
#        vehicle registration
#   2/1/2021 - update apt_surrendered to parse status_codes for code of 'x' = 'Estate - Vacant' & create nested Case statement in place of multiple and expressions
#               - Code 'Estate - Vacant' should appear only when 'Moved' and 'External Move' are also coded, but allow for data entry error...

#   prod environment:
#     mysql --defaults-file=/home/pennsouthdata/.my.cnf  -D pennsout_db -h 127.0.0.1 <<STOP
#   dev environment:
#     mysql --defaults-file=/Users/sfrizell/.my.cnf -D pennsout_db -h 127.0.0.1 <<STOP

mysql --defaults-file=/Users/sfrizell/.my.cnf -D pennsout_db -h 127.0.0.1 <<STOP

--  maintain audit trail of time script takes to run...
SET @start=UNIX_TIMESTAMP();

--  Truncate and load mds_export file into mds_export 

TRUNCATE mds_export;

LOAD DATA LOCAL INFILE "./data/mds_export.csv"
INTO TABLE mds_export 
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
IGNORE 1 LINES
( Building, MDS_Apt, First_Name, Last_Name, Email_Address, Category, @Daytime_Phone, @Evening_Phone,
	@Cell_Phone, @Fax, Tenant_Id, Person_Id,@Date_of_Birth, @Decal_Num, @Vehicle_Reg_Exp_Date,
    @Homeowner_Insurance_Exp_Date, Storage_Locker_Closet_Bldg_Num, @Storage_Locker_Num,
    @Storage_Closet_Floor_Num, Dog_Tag_Num, Bike_Rack_Location, Bike_Rack_Bldg, Bike_Rack_Room,
    vehicle_model, vehicle_license_plate_num,
     Status_Codes, Standard_Lockbox_Tenant_Id, @move_in_date,
     shareholder_flag, @inc_affidavit_receipt_date, inc_affidavit_received, inc_affidavit_date_discrepancy,
     @vacate_date, hperson_id,
     @apt_surrendered,
     @Toddler_Room_Member, @Youth_Room_Member, 
    @Ceramics_Member, @Garden_Member, @Woodworking_Member, @Gym_Member, @Floor_Number, @Apt_Line)
  set 
   Move_In_Date = if(length(trim(@move_in_date)) = 0, NULL, STR_TO_DATE(@move_in_date, '%m/%d/%Y')),
   Daytime_Phone = if(LENGTH(@Daytime_Phone) < 10, NULL, REPLACE(@Daytime_Phone, ' ', '')),
   Evening_Phone = if(LENGTH(@Evening_Phone) < 10, NULL, REPLACE(@Evening_Phone, ' ', '')),
   Cell_Phone = if(LENGTH(@Cell_Phone) < 10, NULL, REPLACE(@Cell_Phone, ' ', '')),
   Fax = if(LENGTH(@Fax) < 10, NULL, REPLACE(@Fax, ' ', '')),
   Date_Of_Birth = if(length(trim(@Date_Of_Birth)) = 0, NULL, STR_TO_DATE(@Date_Of_Birth, '%m/%d/%y')),
   Decal_Num = if(LOCATE('M', @Decal_Num) > 0, SUBSTR(@Decal_Num, 1, LOCATE('M', @Decal_Num)-1), If(LOCATE('G', @Decal_Num) > 0, SUBSTR(@Decal_Num, 1, LOCATE('G', @Decal_Num)-1), @Decal_Num)) ,
   Vehicle_Reg_Exp_Date = if (length(trim(@Vehicle_Reg_Exp_Date)) = 0, NULL, STR_TO_DATE(@Vehicle_Reg_Exp_Date, '%m/%d/%Y')),
   Homeowner_Insurance_Exp_Date = if (length(trim(@Homeowner_Insurance_Exp_Date)) = 0, NULL, STR_TO_DATE(@Homeowner_Insurance_Exp_Date, '%m/%d/%Y')),
   Storage_Locker_Num = if(length(trim(@Storage_Locker_Num)) = 0, NULL, replace(@Storage_Locker_Num, '.00', '')),
   Storage_Closet_Floor_Num = if(length(trim(@Storage_Closet_Floor_Num)) = 0, NULL, replace(@Storage_Closet_Floor_Num, '.00', '')),
   Last_Changed_Date = CURRENT_TIMESTAMP(),
   inc_affidavit_receipt_date = if (length(trim(@inc_affidavit_receipt_date)) = 0, NULL, STR_TO_DATE(@inc_affidavit_receipt_date, '%m/%d/%Y')),
    apt_surrendered =
    (CASE
    	WHEN (INSTR(binary status_codes, 'M') > 0)
    	THEN
    		(CASE
    			WHEN ( (INSTR(binary status_codes, '*') > 0) and  (INSTR(status_codes, 'x') > 0 ))
    				THEN 'Moved; External Move; Estate - Vacant'
    			WHEN ( (INSTR(binary status_codes, '*') > 0) )
    				THEN 'Moved; External Move'
    			WHEN ( (INSTR(binary status_codes, '&') > 0) and ( INSTR( status_codes, 'x') > 0 ))
    				THEN 'Moved; Internal Move; Estate - Vacant'
    			WHEN ( (INSTR(binary status_codes, 'x') > 0) )
    				THEN 'Moved; Estate Vacant'
    			WHEN ( (INSTR(binary status_codes, '&') > 0) )
    				THEN 'Moved; Internal Move'
    			ELSE
    				'Moved'
    		END)
    	ELSE
    		(CASE
    			WHEN ( (INSTR(binary status_codes, '*') > 0) and (INSTR(status_codes, 'x') > 0 ))
    				THEN 'External Move; Estate - Vacant'
    			WHEN ( (INSTR(binary status_codes, '*') > 0) )
    				THEN 'External Move'
    			WHEN ( (INSTR(status_codes, 'x') > 0) )
    				THEN 'Estate Vacant'
    			WHEN ( (INSTR(binary status_codes, '&') > 0) and (INSTR( status_codes, 'x') > 0 ))
    				THEN 'Internal Move; Estate - Vacant'
    			WHEN ( (INSTR(binary status_codes, '&') > 0) )
    				THEN 'Internal Move'
    			ELSE
    				''
    		END)
     END),
   Toddler_Room_Member = if (Instr(Status_Codes, '7') > 0, 'Y', NULL),
   Youth_Room_Member = if (Instr(Status_Codes, 'k') > 0, 'Y', NULL),
   Ceramics_Member = if (Instr(Category, 'CERAMICS_FULL_MBR') > 0, 'Y', NULL),
   Garden_Member = if (Instr(Category, 'GARDEN_MBR') > 0, 'Y', NULL),
   Woodworking_Member = if (Instr(Category, 'WOODWORKING_MBR') > 0, 'Y', NULL),
   Gym_Member = if (Instr(Category, 'GYM_MBR') > 0, 'Y', NULL), 
   category_interpreted = if (Instr(Category, 'SHAREHOLDER') > 0, 'SHAREHOLDER', if (Instr(Category, 'DECEASED') > 0, 'DECEASED', if (length(trim(Category)) = 0, '', 
   if (Instr(Category, 'NON-RES') > 0, 'NONRESIDENT', 'OCCUPANT')))),
   Floor_Number = if (LENGTH(MDS_Apt) = 2, substr(MDS_Apt, 1, 1), substr(MDS_Apt, 1, 2) ),
   Apt_Line = if (LENGTH(MDS_Apt) = 2, substr(MDS_Apt, 2, 1), substr(MDS_Apt, 3, 1) );



--  Truncate and load pennsouth_resident table...

/**
 script: insert_pennsouth_resident.sql
 author: s. frizell
 date: 9/25/2016
 function: 
		Populate the pennsouth_resident table from the Mds_export table. Where there are 2 email addresses in a given row in 
			Mds_export (separator is a semi-colon in the email_address column), insert a separate row for each email address.
		Populate the pennsouth_resident table as follows:
            1) truncate the table
            2) insert all rows from mds_export table that has a value in the email_address column and there is only one value in the column (i.e., no semi-colon is found)
            3) insert all rows from mds_export table that has a value in the email_address column and there is more than one email address in the column - get the first email address
            4) insert all rows from mds_export table that has a value in the email_address column and there is more than one email address in the column - get the first email address
            5) insert all rows from mds_export table where the mds_export.email_address column is null.
    5/2/2017: 
       - Updated to include new pennsouth_resident columns for income affidavit, etc.
**/

truncate table pennsouth_resident;

-- 1
-- insert into pennsouth_resident where there is no more than 1 email address defined for the resident in the MDS_Export table
insert ignore into pennsouth_resident
(pennsouth_apt_apartment_id, building, floor_number, apt_line, last_name, first_name, email_address, MDS_Resident_Category, daytime_phone, evening_phone, cell_phone, 
	fax,  Person_Id, Toddler_Room_Member, Youth_Room_Member, Ceramics_Member, Woodworking_Member,
    Gym_Member, Garden_Member, Decal_Num, Parking_Lot_Location, Vehicle_Reg_Exp_Date, Vehicle_Reg_Exp_Countdown, vehicle_reg_interval_remaining,
    vehicle_model, vehicle_license_plate_num,
    Homeowner_Ins_Exp_Date, Homeowner_Ins_Exp_Countdown, homeowner_ins_interval_remaining, Birth_Date, Move_In_Date, 
    shareholder_flag, inc_affidavit_receipt_date, inc_affidavit_received, inc_affidavit_date_discrepancy, apt_surrendered, mds_export_id,
    hperson_id, Storage_Locker_Closet_Bldg_Num,
    Storage_Locker_Num, Storage_Closet_Floor_Num, Dog_Tag_Num, Is_Dog_In_Apt, Bike_Rack_Location, Bike_Rack_Bldg, Bike_Rack_Room,
    last_changed_date)
select  apt.apartment_id, me.building,  me.floor_number, me.apt_line,
	if(me.last_name is null, '', trim(me.last_name)), if(me.first_name is null, '', trim(me.first_name)),
    if(me.email_address is null, '', trim(me.email_address)),
    if(me.Category_interpreted is null, '', trim(me.Category_interpreted)), if(me.daytime_phone is null, '', trim(me.daytime_phone)),
	if(me.evening_phone is null, '', trim(me.evening_phone)), if(me.cell_phone is null, '', trim(me.cell_phone)), if(me.fax is null, '', trim(me.fax)),
    if(me.person_id is null, '', trim(me.person_id)), if(me.Toddler_Room_Member is null, '', trim(me.Toddler_Room_Member)),
    if(me.Youth_Room_Member is null, '', trim(me.Youth_Room_Member)), if(me.Ceramics_Member is null, '', trim(me.Ceramics_Member)),
    if(me.Woodworking_Member is null, '', trim(me.Woodworking_Member)), if(me.Gym_Member is null, '', trim(me.Gym_Member)),
    if(me.Garden_Member is null, '', trim(me.Garden_Member)), if (length(trim(me.Decal_Num)) = 0, NULL, me.Decal_Num),
    CASE
		WHEN length(trim(me.Decal_Num)) = 0 then ''
        WHEN me.Decal_Num > 0 and me.Decal_Num < 300 then 'LOWER'
        WHEN me.Decal_Num > 299 then 'UPPER'
        ELSE ''
    END,
    me.Vehicle_Reg_Exp_Date, if (me.Vehicle_Reg_Exp_Date is null, null, DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate())),
    if (me.Vehicle_Reg_Exp_Date is null, null,
		(CASE
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    if(me.vehicle_model is null, '', trim(me.vehicle_model)),
    if(me.vehicle_license_plate_num is null, '', trim(me.vehicle_license_plate_num)),
    me.Homeowner_Insurance_Exp_Date, if (me.Homeowner_Insurance_Exp_Date is null, NULL, DATEDIFF(me.Homeowner_Insurance_Exp_Date, CurDate())),
    if (me.homeowner_insurance_exp_date is null, null,
		(CASE
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    me.Date_Of_Birth, me.Move_In_Date, me.shareholder_flag, me.inc_affidavit_receipt_date, me.inc_affidavit_received, me.inc_affidavit_date_discrepancy,
    me.apt_surrendered, me.mds_export_id, me.hperson_id,
    if(me.Storage_Locker_Closet_Bldg_Num is null, '', trim(me.Storage_Locker_Closet_Bldg_Num)),
    if(me.Storage_Locker_Num is null, '', trim(me.Storage_Locker_Num)) , if(me.Storage_Closet_Floor_Num is null, '', trim(me.Storage_Closet_Floor_Num)),
    if(me.Dog_Tag_Num is null, '', trim(me.Dog_Tag_Num)),
    if(length(trim(me.Dog_Tag_Num)) = 0, '', 'Y'), if(me.Bike_Rack_Location is null, '', trim(me.Bike_Rack_Location)),
    if(me.Bike_Rack_Bldg is null, '', trim(me.Bike_Rack_Bldg)), if(me.Bike_Rack_Room is null, '', trim(me.Bike_Rack_Room)),
     sysdate()
from
	 pennsouth_apt as apt
     inner join mds_export as me
where
	apt.building_id = me.building
and apt.floor_number = me.floor_number
and apt.apt_line	= me.apt_line
	AND LENGTH(TRIM(EMAIL_ADDRESS)) > 0
and LOCATE(';', EMAIL_ADDRESS) = 0;



-- 2
-- insert into pennsouth_resident where Mds_export.email_address has 2 email addresses. Insert the 1st email address, located before the semi-colon
insert ignore into pennsouth_resident
(pennsouth_apt_apartment_id, building, floor_number, apt_line, last_name, first_name, email_address, MDS_Resident_Category, daytime_phone, evening_phone, cell_phone,
	fax, Person_Id, Toddler_Room_Member, Youth_Room_Member, Ceramics_Member, Woodworking_Member,
    Gym_Member, Garden_Member, Decal_Num, Parking_Lot_Location, Vehicle_Reg_Exp_Date, Vehicle_Reg_Exp_Countdown, vehicle_reg_interval_remaining,
    vehicle_model, vehicle_license_plate_num,
    Homeowner_Ins_Exp_Date, Homeowner_Ins_Exp_Countdown, homeowner_ins_interval_remaining, Birth_Date, Move_In_Date,
    shareholder_flag, inc_affidavit_receipt_date, inc_affidavit_received, inc_affidavit_date_discrepancy, apt_surrendered, mds_export_id,
    hperson_id, Storage_Locker_Closet_Bldg_Num,
    Storage_Locker_Num, Storage_Closet_Floor_Num, Dog_Tag_Num, Is_Dog_In_Apt, Bike_Rack_Location, Bike_Rack_Bldg, Bike_Rack_Room,
    last_changed_date)
select  apt.apartment_id, me.building,  me.floor_number, me.apt_line,
	if(me.last_name is null, '', me.last_name), if(me.first_name is null, '', me.first_name),
	trim(SUBSTR(me.email_address, 1, (LOCATE(';', me.EMAIL_ADDRESS))-1)) email_address,
    if(me.Category_interpreted is null, '', trim(me.Category_interpreted)), if(me.daytime_phone is null, '', trim(me.daytime_phone)),
	if(me.evening_phone is null, '', trim(me.evening_phone)), if(me.cell_phone is null, '', trim(me.cell_phone)), if(me.fax is null, '', trim(me.fax)),
    if(me.person_id is null, '', trim(me.person_id)), if(me.Toddler_Room_Member is null, '', trim(me.Toddler_Room_Member)),
    if(me.Youth_Room_Member is null, '', trim(me.Youth_Room_Member)), if(me.Ceramics_Member is null, '', trim(me.Ceramics_Member)),
    if(me.Woodworking_Member is null, '', trim(me.Woodworking_Member)), if(me.Gym_Member is null, '', trim(me.Gym_Member)),
    if(me.Garden_Member is null, '', trim(me.Garden_Member)), if (length(trim(me.Decal_Num)) = 0, NULL, me.Decal_Num),
    CASE
		WHEN length(trim(me.Decal_Num)) = 0 then ''
        WHEN me.Decal_Num > 0 and me.Decal_Num < 300 then 'LOWER'
        WHEN me.Decal_Num > 299 then 'UPPER'
        ELSE ''
    END,
	me.Vehicle_Reg_Exp_Date, if (me.Vehicle_Reg_Exp_Date is null, null, DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate())),
    if (me.Vehicle_Reg_Exp_Date is null, null,
		(CASE
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    if(me.vehicle_model is null, '', trim(me.vehicle_model)),
    if(me.vehicle_license_plate_num is null, '', trim(me.vehicle_license_plate_num)),
    me.Homeowner_Insurance_Exp_Date, if (me.Homeowner_Insurance_Exp_Date is null, NULL, DATEDIFF(me.Homeowner_Insurance_Exp_Date, CurDate())),
    if (me.homeowner_insurance_exp_date is null, null,
		(CASE
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    me.Date_Of_Birth, me.Move_In_Date,  me.shareholder_flag, me.inc_affidavit_receipt_date, me.inc_affidavit_received, me.inc_affidavit_date_discrepancy,
    me.apt_surrendered, me.mds_export_id, me.hperson_id,
    if(me.Storage_Locker_Closet_Bldg_Num is null, '', trim(me.Storage_Locker_Closet_Bldg_Num)),
    if(me.Storage_Locker_Num is null, '', trim(me.Storage_Locker_Num)) , if(me.Storage_Closet_Floor_Num is null, '', trim(me.Storage_Closet_Floor_Num)),
    if(me.Dog_Tag_Num is null, '', trim(me.Dog_Tag_Num)),
    if(length(trim(me.Dog_Tag_Num)) = 0, '', 'Y'), if(me.Bike_Rack_Location is null, '', trim(me.Bike_Rack_Location)),
    if(me.Bike_Rack_Bldg is null, '', trim(me.Bike_Rack_Bldg)), if(me.Bike_Rack_Room is null, '', trim(me.Bike_Rack_Room)),
     sysdate()
from
	 pennsouth_apt as apt
     inner join mds_export as me
where
	apt.building_id = me.building
and apt.floor_number = me.floor_number
and apt.apt_line	= me.apt_line
and length(trim(trailing ';' from me.email_address)) - length (replace(me.email_address, ';', ''))  = 1;


-- 3
-- insert into pennsouth_resident where Mds_export.email_address has 2 email addresses. Insert the 2nd email address, located after the semi-colon
-- SUBSTR(me.email_address, (LOCATE(';', me.EMAIL_ADDRESS))+1) email_address,
insert ignore into pennsouth_resident
(pennsouth_apt_apartment_id, building, floor_number, apt_line, last_name, first_name, email_address, MDS_Resident_Category, daytime_phone, evening_phone, cell_phone,
	fax, Person_Id, Toddler_Room_Member, Youth_Room_Member, Ceramics_Member, Woodworking_Member,
    Gym_Member, Garden_Member, Decal_Num, Parking_Lot_Location, Vehicle_Reg_Exp_Date, Vehicle_Reg_Exp_Countdown, vehicle_reg_interval_remaining,
    vehicle_model, vehicle_license_plate_num,
    Homeowner_Ins_Exp_Date, Homeowner_Ins_Exp_Countdown, homeowner_ins_interval_remaining, Birth_Date, Move_In_Date,
    shareholder_flag, inc_affidavit_receipt_date, inc_affidavit_received, inc_affidavit_date_discrepancy, apt_surrendered, mds_export_id,
    hperson_id, Storage_Locker_Closet_Bldg_Num,
    Storage_Locker_Num, Storage_Closet_Floor_Num, Dog_Tag_Num, Is_Dog_In_Apt, Bike_Rack_Location, Bike_Rack_Bldg, Bike_Rack_Room,
    last_changed_date)
select  apt.apartment_id, me.building,  me.floor_number, me.apt_line,
	if(me.last_name is null, '', me.last_name), if(me.first_name is null, '', me.first_name),
	trim(trim(trailing ';' from SUBSTR(me.email_address, (LOCATE(';', me.EMAIL_ADDRESS))+1))) email_address,
    if(me.Category_interpreted is null, '', trim(me.Category_interpreted)), if(me.daytime_phone is null, '', trim(me.daytime_phone)),
	if(me.evening_phone is null, '', trim(me.evening_phone)), if(me.cell_phone is null, '', trim(me.cell_phone)), if(me.fax is null, '', trim(me.fax)),
    if(me.person_id is null, '', trim(me.person_id)), if(me.Toddler_Room_Member is null, '', trim(me.Toddler_Room_Member)),
    if(me.Youth_Room_Member is null, '', trim(me.Youth_Room_Member)), if(me.Ceramics_Member is null, '', trim(me.Ceramics_Member)),
    if(me.Woodworking_Member is null, '', trim(me.Woodworking_Member)), if(me.Gym_Member is null, '', trim(me.Gym_Member)),
    if(me.Garden_Member is null, '', trim(me.Garden_Member)), if (length(trim(me.Decal_Num)) = 0, NULL, me.Decal_Num),
    CASE
		WHEN length(trim(me.Decal_Num)) = 0 then ''
        WHEN me.Decal_Num > 0 and me.Decal_Num < 300 then 'LOWER'
        WHEN me.Decal_Num > 299 then 'UPPER'
        ELSE ''
    END,
    me.Vehicle_Reg_Exp_Date, if (me.Vehicle_Reg_Exp_Date is null, null, DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate())),
	if (me.Vehicle_Reg_Exp_Date is null, null,
		(CASE
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    if(me.vehicle_model is null, '', trim(me.vehicle_model)),
    if(me.vehicle_license_plate_num is null, '', trim(me.vehicle_license_plate_num)),
    me.Homeowner_Insurance_Exp_Date, if (me.Homeowner_Insurance_Exp_Date is null, NULL, DATEDIFF(me.Homeowner_Insurance_Exp_Date, CurDate())),
    if (me.homeowner_insurance_exp_date is null, null,
		(CASE
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    me.Date_Of_Birth, me.Move_In_Date,  me.shareholder_flag, me.inc_affidavit_receipt_date, me.inc_affidavit_received, me.inc_affidavit_date_discrepancy,
    me.apt_surrendered, me.mds_export_id, me.hperson_id,
    if(me.Storage_Locker_Closet_Bldg_Num is null, '', trim(me.Storage_Locker_Closet_Bldg_Num)),
    if(me.Storage_Locker_Num is null, '', trim(me.Storage_Locker_Num)) , if(me.Storage_Closet_Floor_Num is null, '', trim(me.Storage_Closet_Floor_Num)),
    if(me.Dog_Tag_Num is null, '', trim(me.Dog_Tag_Num)),
    if(length(trim(me.Dog_Tag_Num)) = 0, '', 'Y'), if(me.Bike_Rack_Location is null, '', trim(me.Bike_Rack_Location)),
    if(me.Bike_Rack_Bldg is null, '', trim(me.Bike_Rack_Bldg)), if(me.Bike_Rack_Room is null, '', trim(me.Bike_Rack_Room)),
     sysdate()
from
	 pennsouth_apt as apt
     inner join mds_export as me
where
	apt.building_id = me.building
and apt.floor_number = me.floor_number
and apt.apt_line	= me.apt_line
and length(trim(trailing ';' from me.email_address)) - length (replace(me.email_address, ';', ''))  = 1;

-- 4
-- insert into pennsouth_resident where Mds_export.email_address has 3 email addresses. Insert the 1st email address, located after the semi-colon
-- SUBSTR(me.email_address, (LOCATE(';', me.EMAIL_ADDRESS))+2) email_address,
insert ignore into pennsouth_resident
(pennsouth_apt_apartment_id, building, floor_number, apt_line, last_name, first_name, email_address, MDS_Resident_Category, daytime_phone, evening_phone, cell_phone,
	fax, Person_Id, Toddler_Room_Member, Youth_Room_Member, Ceramics_Member, Woodworking_Member,
    Gym_Member, Garden_Member, Decal_Num, Parking_Lot_Location, Vehicle_Reg_Exp_Date, Vehicle_Reg_Exp_Countdown, vehicle_reg_interval_remaining,
    vehicle_model, vehicle_license_plate_num,
    Homeowner_Ins_Exp_Date, Homeowner_Ins_Exp_Countdown, homeowner_ins_interval_remaining, Birth_Date, Move_In_Date,
    shareholder_flag, inc_affidavit_receipt_date, inc_affidavit_received, inc_affidavit_date_discrepancy, apt_surrendered, mds_export_id,
    hperson_id, Storage_Locker_Closet_Bldg_Num,
    Storage_Locker_Num, Storage_Closet_Floor_Num, Dog_Tag_Num, Is_Dog_In_Apt, Bike_Rack_Location, Bike_Rack_Bldg, Bike_Rack_Room,
    last_changed_date)
select  apt.apartment_id, me.building,  me.floor_number, me.apt_line,
	if(me.last_name is null, '', me.last_name), if(me.first_name is null, '', me.first_name),
	trim(SUBSTR(me.email_address, 1, (LOCATE(';', me.EMAIL_ADDRESS))-1)) email_address,
    if(me.Category_interpreted is null, '', trim(me.Category_interpreted)), if(me.daytime_phone is null, '', trim(me.daytime_phone)),
	if(me.evening_phone is null, '', trim(me.evening_phone)), if(me.cell_phone is null, '', trim(me.cell_phone)), if(me.fax is null, '', trim(me.fax)),
    if(me.person_id is null, '', trim(me.person_id)), if(me.Toddler_Room_Member is null, '', trim(me.Toddler_Room_Member)),
    if(me.Youth_Room_Member is null, '', trim(me.Youth_Room_Member)), if(me.Ceramics_Member is null, '', trim(me.Ceramics_Member)),
    if(me.Woodworking_Member is null, '', trim(me.Woodworking_Member)), if(me.Gym_Member is null, '', trim(me.Gym_Member)),
    if(me.Garden_Member is null, '', trim(me.Garden_Member)), if (length(trim(me.Decal_Num)) = 0, NULL, me.Decal_Num),
    CASE
		WHEN length(trim(me.Decal_Num)) = 0 then ''
        WHEN me.Decal_Num > 0 and me.Decal_Num < 300 then 'LOWER'
        WHEN me.Decal_Num > 299 then 'UPPER'
        ELSE ''
    END,
	me.Vehicle_Reg_Exp_Date, if (me.Vehicle_Reg_Exp_Date is null, null, DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate())),
    if (me.Vehicle_Reg_Exp_Date is null, null,
		(CASE
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    if(me.vehicle_model is null, '', trim(me.vehicle_model)),
    if(me.vehicle_license_plate_num is null, '', trim(me.vehicle_license_plate_num)),
    me.Homeowner_Insurance_Exp_Date, if (me.Homeowner_Insurance_Exp_Date is null, NULL, DATEDIFF(me.Homeowner_Insurance_Exp_Date, CurDate())),
    if (me.homeowner_insurance_exp_date is null, null,
		(CASE
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    me.Date_Of_Birth, me.Move_In_Date,  me.shareholder_flag, me.inc_affidavit_receipt_date, me.inc_affidavit_received, me.inc_affidavit_date_discrepancy,
    me.apt_surrendered, me.mds_export_id, me.hperson_id,
    if(me.Storage_Locker_Closet_Bldg_Num is null, '', trim(me.Storage_Locker_Closet_Bldg_Num)),
    if(me.Storage_Locker_Num is null, '', trim(me.Storage_Locker_Num)) , if(me.Storage_Closet_Floor_Num is null, '', trim(me.Storage_Closet_Floor_Num)),
    if(me.Dog_Tag_Num is null, '', trim(me.Dog_Tag_Num)),
    if(length(trim(me.Dog_Tag_Num)) = 0, '', 'Y'), if(me.Bike_Rack_Location is null, '', trim(me.Bike_Rack_Location)),
    if(me.Bike_Rack_Bldg is null, '', trim(me.Bike_Rack_Bldg)), if(me.Bike_Rack_Room is null, '', trim(me.Bike_Rack_Room)),
     sysdate()
from
	 pennsouth_apt as apt
     inner join mds_export as me
where
	apt.building_id = me.building
and apt.floor_number = me.floor_number
and apt.apt_line	= me.apt_line
and length(trim(trailing ';' from me.email_address)) - length (replace(me.email_address, ';', ''))  = 2;

-- 5
-- insert into pennsouth_resident where Mds_export.email_address has 3 email addresses. Insert the 2nd email address, located between the 2 semi-colons
--
insert ignore into pennsouth_resident
(pennsouth_apt_apartment_id, building, floor_number, apt_line, last_name, first_name, email_address, MDS_Resident_Category, daytime_phone, evening_phone, cell_phone,
	fax, Person_Id, Toddler_Room_Member, Youth_Room_Member, Ceramics_Member, Woodworking_Member,
    Gym_Member, Garden_Member, Decal_Num, Parking_Lot_Location, Vehicle_Reg_Exp_Date, Vehicle_Reg_Exp_Countdown, vehicle_reg_interval_remaining,
    vehicle_model, vehicle_license_plate_num,
    Homeowner_Ins_Exp_Date, Homeowner_Ins_Exp_Countdown, homeowner_ins_interval_remaining, Birth_Date, Move_In_Date,
    shareholder_flag, inc_affidavit_receipt_date, inc_affidavit_received, inc_affidavit_date_discrepancy, apt_surrendered, mds_export_id,
    hperson_id, Storage_Locker_Closet_Bldg_Num,
    Storage_Locker_Num, Storage_Closet_Floor_Num, Dog_Tag_Num, Is_Dog_In_Apt, Bike_Rack_Location, Bike_Rack_Bldg, Bike_Rack_Room,
    last_changed_date)
select  apt.apartment_id, me.building,  me.floor_number, me.apt_line,
	if(me.last_name is null, '', me.last_name), if(me.first_name is null, '', me.first_name),
	trim(substring_index( substring_index(me.email_address, ';', -2  ), ';', 1)) email_address,
    if(me.Category_interpreted is null, '', trim(me.Category_interpreted)), if(me.daytime_phone is null, '', trim(me.daytime_phone)),
	if(me.evening_phone is null, '', trim(me.evening_phone)), if(me.cell_phone is null, '', trim(me.cell_phone)), if(me.fax is null, '', trim(me.fax)),
    if(me.person_id is null, '', trim(me.person_id)), if(me.Toddler_Room_Member is null, '', trim(me.Toddler_Room_Member)),
    if(me.Youth_Room_Member is null, '', trim(me.Youth_Room_Member)), if(me.Ceramics_Member is null, '', trim(me.Ceramics_Member)),
    if(me.Woodworking_Member is null, '', trim(me.Woodworking_Member)), if(me.Gym_Member is null, '', trim(me.Gym_Member)),
    if(me.Garden_Member is null, '', trim(me.Garden_Member)), if (length(trim(me.Decal_Num)) = 0, NULL, me.Decal_Num),
    CASE
		WHEN length(trim(me.Decal_Num)) = 0 then ''
        WHEN me.Decal_Num > 0 and me.Decal_Num < 300 then 'LOWER'
        WHEN me.Decal_Num > 299 then 'UPPER'
        ELSE ''
    END,
	me.Vehicle_Reg_Exp_Date, if (me.Vehicle_Reg_Exp_Date is null, null, DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate())),
    if (me.Vehicle_Reg_Exp_Date is null, null,
		(CASE
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    if(me.vehicle_model is null, '', trim(me.vehicle_model)),
    if(me.vehicle_license_plate_num is null, '', trim(me.vehicle_license_plate_num)),
    me.Homeowner_Insurance_Exp_Date, if (me.Homeowner_Insurance_Exp_Date is null, NULL, DATEDIFF(me.Homeowner_Insurance_Exp_Date, CurDate())),
    if (me.homeowner_insurance_exp_date is null, null,
		(CASE
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    me.Date_Of_Birth, me.Move_In_Date,  me.shareholder_flag, me.inc_affidavit_receipt_date, me.inc_affidavit_received, me.inc_affidavit_date_discrepancy,
    me.apt_surrendered, me.mds_export_id, me.hperson_id,
    if(me.Storage_Locker_Closet_Bldg_Num is null, '', trim(me.Storage_Locker_Closet_Bldg_Num)),
    if(me.Storage_Locker_Num is null, '', trim(me.Storage_Locker_Num)) , if(me.Storage_Closet_Floor_Num is null, '', trim(me.Storage_Closet_Floor_Num)),
    if(me.Dog_Tag_Num is null, '', trim(me.Dog_Tag_Num)),
    if(length(trim(me.Dog_Tag_Num)) = 0, '', 'Y'), if(me.Bike_Rack_Location is null, '', trim(me.Bike_Rack_Location)),
    if(me.Bike_Rack_Bldg is null, '', trim(me.Bike_Rack_Bldg)), if(me.Bike_Rack_Room is null, '', trim(me.Bike_Rack_Room)),
     sysdate()
from
	 pennsouth_apt as apt
     inner join mds_export as me
where
	apt.building_id = me.building
and apt.floor_number = me.floor_number
and apt.apt_line	= me.apt_line
and length(trim(trailing ';' from me.email_address)) - length (replace(me.email_address, ';', ''))  = 2;

-- 6
-- insert into pennsouth_resident where Mds_export.email_address has 3 email addresses. Insert the 3rd email address, located after the 2nd semi-colon
--
insert ignore into pennsouth_resident
(pennsouth_apt_apartment_id, building, floor_number, apt_line, last_name, first_name, email_address, MDS_Resident_Category, daytime_phone, evening_phone, cell_phone,
	fax, Person_Id, Toddler_Room_Member, Youth_Room_Member, Ceramics_Member, Woodworking_Member,
    Gym_Member, Garden_Member, Decal_Num, Parking_Lot_Location, Vehicle_Reg_Exp_Date, Vehicle_Reg_Exp_Countdown, vehicle_reg_interval_remaining,
    vehicle_model, vehicle_license_plate_num,
    Homeowner_Ins_Exp_Date, Homeowner_Ins_Exp_Countdown, homeowner_ins_interval_remaining, Birth_Date, Move_In_Date,
    shareholder_flag, inc_affidavit_receipt_date, inc_affidavit_received, inc_affidavit_date_discrepancy, apt_surrendered, mds_export_id,
    hperson_id, Storage_Locker_Closet_Bldg_Num,
    Storage_Locker_Num, Storage_Closet_Floor_Num, Dog_Tag_Num, Is_Dog_In_Apt, Bike_Rack_Location, Bike_Rack_Bldg, Bike_Rack_Room,
    last_changed_date)
select  apt.apartment_id, me.building,  me.floor_number, me.apt_line,
	if(me.last_name is null, '', me.last_name), if(me.first_name is null, '', me.first_name),
	trim(substring_index(me.email_address, ';', -1  ))  email_address,
    if(me.Category_interpreted is null, '', trim(me.Category_interpreted)), if(me.daytime_phone is null, '', trim(me.daytime_phone)),
	if(me.evening_phone is null, '', trim(me.evening_phone)), if(me.cell_phone is null, '', trim(me.cell_phone)), if(me.fax is null, '', trim(me.fax)),
    if(me.person_id is null, '', trim(me.person_id)), if(me.Toddler_Room_Member is null, '', trim(me.Toddler_Room_Member)),
    if(me.Youth_Room_Member is null, '', trim(me.Youth_Room_Member)), if(me.Ceramics_Member is null, '', trim(me.Ceramics_Member)),
    if(me.Woodworking_Member is null, '', trim(me.Woodworking_Member)), if(me.Gym_Member is null, '', trim(me.Gym_Member)),
    if(me.Garden_Member is null, '', trim(me.Garden_Member)), if (length(trim(me.Decal_Num)) = 0, NULL, me.Decal_Num),
    CASE
		WHEN length(trim(me.Decal_Num)) = 0 then ''
        WHEN me.Decal_Num > 0 and me.Decal_Num < 300 then 'LOWER'
        WHEN me.Decal_Num > 299 then 'UPPER'
        ELSE ''
    END,
	me.Vehicle_Reg_Exp_Date, if (me.Vehicle_Reg_Exp_Date is null, null, DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate())),
    if (me.Vehicle_Reg_Exp_Date is null, null,
		(CASE
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    if(me.vehicle_model is null, '', trim(me.vehicle_model)),
    if(me.vehicle_license_plate_num is null, '', trim(me.vehicle_license_plate_num)),
    me.Homeowner_Insurance_Exp_Date, if (me.Homeowner_Insurance_Exp_Date is null, NULL, DATEDIFF(me.Homeowner_Insurance_Exp_Date, CurDate())),
    if (me.homeowner_insurance_exp_date is null, null,
		(CASE
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    me.Date_Of_Birth, me.Move_In_Date,  me.shareholder_flag, me.inc_affidavit_receipt_date, me.inc_affidavit_received, me.inc_affidavit_date_discrepancy,
    me.apt_surrendered, me.mds_export_id, me.hperson_id,
    if(me.Storage_Locker_Closet_Bldg_Num is null, '', trim(me.Storage_Locker_Closet_Bldg_Num)),
    if(me.Storage_Locker_Num is null, '', trim(me.Storage_Locker_Num)) , if(me.Storage_Closet_Floor_Num is null, '', trim(me.Storage_Closet_Floor_Num)),
    if(me.Dog_Tag_Num is null, '', trim(me.Dog_Tag_Num)),
    if(length(trim(me.Dog_Tag_Num)) = 0, '', 'Y'), if(me.Bike_Rack_Location is null, '', trim(me.Bike_Rack_Location)),
    if(me.Bike_Rack_Bldg is null, '', trim(me.Bike_Rack_Bldg)), if(me.Bike_Rack_Room is null, '', trim(me.Bike_Rack_Room)),
     sysdate()
from
	 pennsouth_apt as apt
     inner join mds_export as me
where
	apt.building_id = me.building
and apt.floor_number = me.floor_number
and apt.apt_line	= me.apt_line
and length(trim(trailing ';' from me.email_address)) - length (replace(me.email_address, ';', ''))  = 2;


-- 7
-- insert into pennsouth_resident where mds_export.email_address is null
-- NULL email_address,
insert ignore into pennsouth_resident
(pennsouth_apt_apartment_id, building, floor_number, apt_line, last_name, first_name, email_address, MDS_Resident_Category, daytime_phone, evening_phone, cell_phone,
	fax, Person_Id, Toddler_Room_Member, Youth_Room_Member, Ceramics_Member, Woodworking_Member,
    Gym_Member, Garden_Member, Decal_Num, Parking_Lot_Location, Vehicle_Reg_Exp_Date, Vehicle_Reg_Exp_Countdown, vehicle_reg_interval_remaining,
    vehicle_model, vehicle_license_plate_num,
    Homeowner_Ins_Exp_Date, Homeowner_Ins_Exp_Countdown, homeowner_ins_interval_remaining, Birth_Date, Move_In_Date,
    shareholder_flag, inc_affidavit_receipt_date, inc_affidavit_received, inc_affidavit_date_discrepancy, apt_surrendered, mds_export_id,
    hperson_id, Storage_Locker_Closet_Bldg_Num,
    Storage_Locker_Num, Storage_Closet_Floor_Num, Dog_Tag_Num, Is_Dog_In_Apt, Bike_Rack_Location, Bike_Rack_Bldg, Bike_Rack_Room,
    last_changed_date)
select  apt.apartment_id, me.building,  me.floor_number, me.apt_line,
	if(me.last_name is null, '', me.last_name), if(me.first_name is null, '', me.first_name),
	'' email_address,
    if(me.Category_interpreted is null, '', trim(me.Category_interpreted)), if(me.daytime_phone is null, '', trim(me.daytime_phone)),
	if(me.evening_phone is null, '', trim(me.evening_phone)), if(me.cell_phone is null, '', trim(me.cell_phone)), if(me.fax is null, '', trim(me.fax)),
    if(me.person_id is null, '', trim(me.person_id)), if(me.Toddler_Room_Member is null, '', trim(me.Toddler_Room_Member)),
    if(me.Youth_Room_Member is null, '', trim(me.Youth_Room_Member)), if(me.Ceramics_Member is null, '', trim(me.Ceramics_Member)),
    if(me.Woodworking_Member is null, '', trim(me.Woodworking_Member)), if(me.Gym_Member is null, '', trim(me.Gym_Member)),
    if(me.Garden_Member is null, '', trim(me.Garden_Member)), if (length(trim(me.Decal_Num)) = 0, NULL, me.Decal_Num),
    CASE
		WHEN length(trim(me.Decal_Num)) = 0 then ''
        WHEN me.Decal_Num > 0 and me.Decal_Num < 300 then 'LOWER'
        WHEN me.Decal_Num > 299 then 'UPPER'
        ELSE ''
    END,
	me.Vehicle_Reg_Exp_Date, if (me.Vehicle_Reg_Exp_Date is null, null, DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate())),
    if (me.Vehicle_Reg_Exp_Date is null, null,
		(CASE
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.Vehicle_Reg_Exp_Date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    if(me.vehicle_model is null, '', trim(me.vehicle_model)),
    if(me.vehicle_license_plate_num is null, '', trim(me.vehicle_license_plate_num)),
    me.Homeowner_Insurance_Exp_Date, if (me.Homeowner_Insurance_Exp_Date is null, NULL, DATEDIFF(me.Homeowner_Insurance_Exp_Date, CurDate())),
    if (me.homeowner_insurance_exp_date is null, null,
		(CASE
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) < 1 then 0
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 1 AND 14 = 1 then 14
		 WHEN DATEDIFF(me.homeowner_insurance_exp_date, CurDate() ) BETWEEN 15 AND 28 = 1 then 28
		 ELSE null
		END)),
    me.Date_Of_Birth, me.Move_In_Date,  me.shareholder_flag, me.inc_affidavit_receipt_date, me.inc_affidavit_received, me.inc_affidavit_date_discrepancy,
    me.apt_surrendered, me.mds_export_id, me.hperson_id,
    if(me.Storage_Locker_Closet_Bldg_Num is null, '', trim(me.Storage_Locker_Closet_Bldg_Num)),
    if(me.Storage_Locker_Num is null, '', trim(me.Storage_Locker_Num)) , if(me.Storage_Closet_Floor_Num is null, '', trim(me.Storage_Closet_Floor_Num)),
    if(me.Dog_Tag_Num is null, '', trim(me.Dog_Tag_Num)),
    if(length(trim(me.Dog_Tag_Num)) = 0, '', 'Y'), if(me.Bike_Rack_Location is null, '', trim(me.Bike_Rack_Location)),
    if(me.Bike_Rack_Bldg is null, '', trim(me.Bike_Rack_Bldg)), if(me.Bike_Rack_Room is null, '', trim(me.Bike_Rack_Room)),
     sysdate()
from
	 pennsouth_apt as apt
     inner join mds_export as me
where
	apt.building_id = me.building
and apt.floor_number = me.floor_number
and apt.apt_line	= me.apt_line
and LENGTH(TRIM(me.EMAIL_ADDRESS)) = 0;


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
    exit 0
else 
	exit 1
fi 
