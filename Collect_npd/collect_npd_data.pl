#!/usr/local/mccs/perl/bin/perl
use strict;
use IBIS::DBI;
use DateTime;
use IO::Compress::Zip qw(:all);
use Net::SFTP::Foreign;
use MCCS::Config;
use Net::SMTP;
use Carp;
use Getopt::Long;
use IBIS::Log::File;
use Readonly;
use File::Basename;
use File::Spec;
use File::Copy;
use File::Path;

# Define constant to contain fully qualified path name to this script's log file
Readonly my $g_logfile => '/usr/local/mccs/log/npd/' . basename(__FILE__) . '.log';
my $stage;
my $retry;
my $g_weekEndingDate;

use constant DEFAULT_OUTPUT_DIR => '/usr/local/mccs/data/npd/';

# Default verbosity mode
my $g_verbose = 0;

# Instantiate MCCS::Config object
my $g_cfg = new MCCS::Config;

# Retrieve list of email addresses to be used by process
#my $g_emails = $g_cfg->npd_DATA->{emails};  TODO , remove two next lines.
my $g_mails;
$g_emails->{kav} ='kaveh.sari@usmc-mccs.org'; 

# Extract file name to produce for NPD from configuration
my $g_npdFile = $g_cfg->npd_DATA->{FILE_TO_PRODUCE};

# Extract name of file to ftp
my $g_file_to_ftp = $g_cfg->npd_DATA->{FILE_TO_FTP};
my $g_file        = File::Spec->catfile( DEFAULT_OUTPUT_DIR, $g_file_to_ftp );

# Extract number of commands that sales must be in for
my $g_num_of_cmds = $g_cfg->npd_DATA->{NUM_OF_CMDS};

# Initialize variable with current date (as in mm/dd/yy hh:mn:ss AM/PM)
my $g_long_date = `date +"%D %r"`;
chomp($g_long_date);

# Initialize variable with short date for Archiving purposes
my $g_date = `date +%F`;
chomp($g_date);

# Instantiate log file object
my $g_log =
  IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );

#handle command line arguments
my $options = ( GetOptions( 'stage=s' => \$stage, 'retry=s' => \$retry ) );

# Connect to DB
my $g_dbh = IBIS::DBI->connect( dbname => 'rms_r' );
$g_log->info("DB rms_r connected");
$g_log->info("Stage = $stage");

# --------------------------------------
# Check if entire process needs to run
# --------------------------------------
if ( $stage eq 'F' ) {

        $g_log->info("If this is the retry instance of the process but file has been sent before today, log in this fact and exit process");
	if ( $retry eq 'y' and HasFileBeenSent() ) {
		$g_log->info( "Retry run does not need to go any further as required file has already been sftp'd today\n");
		exit;
	}

        $g_log->info("Get date for week ending date for previous merchandising week");
	$g_weekEndingDate = getPrevMerchPeriodEnd();

        $g_log->info("Determine if sales are in for all commands to decide if NPD flat file will be produced");
	if ( areAllCmdsDone() ) {

		$g_log->info("Determine current merchandising_year, period and week");
		my ( $merch_year, $merch_period, $merch_week ) = getCurMerchWeek();

		$g_log->info("Insert records for current merchandising week into the NPD_DATA table");
		insertIntoNPDData( $merch_year, $merch_period, $merch_week );

		$g_log->info("Create SALE5625 flat file");
		createFlatFile( $merch_year, $merch_period, $merch_week );

		$g_log->info("Zip up and sftp flat file");
		sftpTONPD();

	} else {
		$g_log->info("Handle Sales Not Ready");
		HandleSalesNotReady();
	}

} else {

        $g_log->info("Just sftp the flat file");
	sftpTONPD();
}

$g_log->info("DONE");

#------------------------------------------------------------------------------
# Determine previous Saturday date (previous merchandising week ending date)
#------------------------------------------------------------------------------
sub getPrevMerchPeriodEnd {
	my $d = DateTime->now;

	while ( $d->day_of_week != 6 ) {
		$d->subtract( days => 1 );
	}

	return $d->ymd;
}

#------------------------------------------------------------------------------
# Determine if the sales for all relevant commands are all in
#------------------------------------------------------------------------------
sub areAllCmdsDone {

        $g_log->info("at SUB  areAllCmdsDone");

	# SQL to detemine number of commands for which sales colletion has completed
	my $cmd_sql = <<CMDCOMPSQL;
select count(*) cmd_cntr from
(SELECT substr(site_id,1,2),
  sum(CASE             WHEN update_status = 'PEND'
                                    THEN 1
                                    END) PEND_TRN,
  sum(CASE             WHEN update_status = 'EXEC'
                                    THEN 1
                                    END) EXEC_TRN,
  sum(CASE             WHEN update_status = 'COMP'
                                    THEN 1
                                    END) COMP_TRN
  FROM sales
  WHERE business_unit_id = 30 AND
  sale_date = to_date(?,'yyyy-mm-dd')
  GROUP BY substr (site_id,1,2)) SAL_DETAIL
  where (PEND_TRN is null and EXEC_TRN is null and COMP_TRN is not null)
CMDCOMPSQL

	my $sth = $g_dbh->prepare($cmd_sql);
	$sth->execute($g_weekEndingDate);
	my $rec = $sth->fetchrow_hashref();

        $g_log->info("");
        $g_log->info("\tcmd_ctr       = " . $rec->{cmd_cntr});
        $g_log->info("\tg_num_of_cmds = $g_num_of_cmds");
        $g_log->info("\t  Note both values above must be the same if you want to run the rest");
        $g_log->info("");

	if ( $rec->{cmd_cntr} == $g_num_of_cmds ) {
		return 1;
	}
	else {
		return 0;
	}
}
#------------------------------------------------------------------------------------
# Figure out current merchandising_year, merchandising_period and merchandising_week
# and return it to calling code
#------------------------------------------------------------------------------------
sub getCurMerchWeek {

	# SQL to get merchandising_year, merchandising_period and merchandising_week
	my $curMerchWeek = <<CURMERWEEK;
select merchandising_year,merchandising_period, merchandising_week
  from merchandising_calendars where business_unit_id = 30
  and trunc(week_ending_date) = to_date(?,'yyyy-mm-dd')
CURMERWEEK

	my $sth = $g_dbh->prepare($curMerchWeek);
	$sth->execute($g_weekEndingDate);
	my $rec = $sth->fetchrow_hashref();
	return (
		$rec->{merchandising_year},
		$rec->{merchandising_period},
		$rec->{merchandising_week}
	);
}

#------------------------------------------------------------------------------------
# Insert current merchandising week data into NPD_DATA table
#------------------------------------------------------------------------------------
sub insertIntoNPDData {
	my $merch_year   = shift;
	my $merch_period = shift;
	my $merch_week   = shift;

	my $npdInsert = <<NPDINSERT;
INSERT INTO npd_data
SELECT  v.department_id,
v.class_id,
v.sub_class_id,
NULL sub_sub_class_id,
sd.style_id||sd.color_id||sd.size_id||sd.dimension_id sku,
sd.style_id,
c.description,
sd.size_id,
sd.dimension_id,
s.description,
'each' uom,
NULL gender,
vv.name,
vv.vendor_id,
NULL brand,
s.vendor_style_no,
sd.bar_code_id,
sum (sd.qty) unit_sales,
get_permanent_retail_price ('30',sd.site_id,sd.style_id,NULL,NULL,NULL,m.week_ending_date,NULL) retail_price,   sum(sd.extension_amount) retail_sales,
get_qty_onhand_new('30',sd.style_id,sd.color_id,sd.dimension_id,sd.size_id,sd.site_id) on_hand,
CASE 	WHEN department_id IN ('0162','0381','0472','0450','0422')
		THEN '1'
		ELSE '2'
END private_label_flag,
NULL price_multiple,
NULL multi_pack_ind,
'09' selling_channel,
A.state_id state,
'US'country_code,
sd.site_id,
A.zip_code,
v.dept_name,
v.class_descr,
v.sub_class_descr,
NULL sub_sub_class_descr,
NULL game_platform,
m.merchandising_year,
m.merchandising_period,
m.merchandising_week
FROM sale_details sd,
styles s,
bar_codes b,
v_dept_class_subclass v,
merch_cal_mike_week\@mc2p m,
sites A,
vendors vv,
colors c
WHERE sd.business_unit_id = 30 AND
sd.business_unit_id = s.business_unit_id AND
sd.business_unit_id = b.business_unit_id AND
sd.business_unit_id = v.business_unit_id AND
sd.business_unit_id = m.business_unit_id AND
sd.business_unit_id = A.business_unit_id AND
sd.business_unit_id = c.business_unit_id AND
sd.business_unit_id = vv.business_unit_id AND
s.vendor_id = vv.vendor_id AND
sd.site_id = A.site_id AND
sd.sub_type = 'ITEM' AND
m.merchandising_year = ? AND
m.merchandising_period = ? AND
m.merchandising_week = ? AND
sd.style_id = s.style_id AND
sd.style_id = b.style_id AND
sd.bar_code_id = b.bar_code_id AND
sd.color_id = c.color_id AND
sd.color_id = b.color_id AND
s.section_id = v.section_id AND
substr(sd.site_id,1,2) NOT IN ( '15','16') AND
v.department_id IN
    (SELECT DISTINCT  v.department_id
    FROM 	v_dept_class_subclass v
    WHERE 	v.business_unit_id = 30  AND
     	 (SELECT m.description
    	  FROM merchandising_levels m
     	 WHERE m.business_unit_id = 30 AND  m.level_id =
     		   (SELECT m.mlvl_level_id 
       		 FROM merchandising_levels m, departments d
WHERE m.business_unit_id = 30 AND
m.business_unit_id = d.business_unit_id AND
d.department_id = v.department_id AND
m.level_id = d.mlvl_level_id)) IN ('Hardlines','Softlines') OR
    (SELECT d.mlvl_level_id 
    FROM departments d
    WHERE d.business_unit_id =30 AND 
d.business_unit_id = v.business_unit_id AND
d.department_id = v.department_id) = '506') AND
sd.sale_date BETWEEN m.week_starting_date AND week_ending_date
    GROUP BY	v.business_unit_id,
A.state_id,
A.zip_code,
A.country_id,
m.merchandising_year,
A.zip_code,
m.merchandising_period,
sd.site_id,
sd.style_id,
vv.vendor_id,
sd.color_id,
sd.size_id,
sd.dimension_id,
A.name,
sd.bar_code_id,
s.vendor_style_no,
s.description,
v.department_id,
v.dept_name,
vv.name,
v.class_id,
v.class_descr,
v.sub_class_id,
v.sub_class_descr,
m.merchandising_week,
m.week_ending_date,
c.description
  HAVING	sum(sd.qty) != 0
NPDINSERT

	my $sth = $g_dbh->prepare($npdInsert);
	$sth->execute( $merch_year, $merch_period, $merch_week );
}

#------------------------------------------------------------------------------------
# Create flat file for current merchandising week
#------------------------------------------------------------------------------------
sub createFlatFile {
	my $merch_year   = shift;
	my $merch_period = shift;
	my $merch_week   = shift;

	my $flatFileData = <<NPDDETAILFLATFILE;
select        N.DEPARTMENT_ID,
              N.CLASS_ID,
              N.SUB_CLASS_ID,
              N.SUB_SUB_CLASS_ID,
              SKU,
              STYLE_ID,
              COLOR,
              SIZE_ID,
              DIMENSION_ID,
              DESCRIPTION,
              UOM,
              GENDER,
              VENDOR_NAME,
              VENDOR_ID,
              BRAND,
              VENDOR_STYLE_NO,
              BAR_CODE_ID,
              UNIT_SALES,
              RETAIL_PRICE,
              RETAIL_SALES,
              ON_HAND,
              PRIVATE_LABEL_FLAG,
              PRICE_MULTIPLE,
              MULTI_PACK_IND,
              SELLING_CHANNEL,
              STATE,
              COUNTRY_CODE,
              SITE_ID,
              ZIP_CODE,
              DEPT_NAME,
              CLASS_DESCR,
              SUB_CLASS_DESCR,
              SUB_SUB_CLASS_DESCR,
              GAME_PLATFORM
FROM          NPD_DATA N
WHERE         MERCHANDISING_YEAR = ? 
AND
              MERCHANDISING_PERIOD = ?
AND
              MERCHANDISING_WEEK = ?              
NPDDETAILFLATFILE

# Execute SQL above to create a recordset containing all the NPD_DATA rows for current merchandising period
	my $sth = $g_dbh->prepare($flatFileData);
	$sth->execute( $merch_year, $merch_period, $merch_week );

	# Create flat file to be sent to NPD
	open OUT, '>', DEFAULT_OUTPUT_DIR . $g_npdFile;

	# Write records to flat file
	my $row;
	while ( $row = $sth->fetchrow_hashref() ) {
		print OUT $row->{department_id} . '|';
		print OUT $row->{class_id} . '|';
		print OUT $row->{sub_class_id} . '|';
		print OUT $row->{sub_sub_class_id} . '|';
		print OUT $row->{sku} . '|';
		print OUT $row->{style_id} . '|';
		print OUT $row->{color} . '|';
		print OUT $row->{size_id} . '|';
		print OUT $row->{dimension_id} . '|';
		print OUT $row->{description} . '|';
		print OUT $row->{uom} . '|';
		print OUT $row->{gender} . '|';
		print OUT $row->{vendor_name} . '|';
		print OUT $row->{vendor_id} . '|';
		print OUT $row->{brand} . '|';
		print OUT $row->{vendor_style_no} . '|';
		print OUT $row->{bar_code_id} . '|';
		print OUT $row->{unit_sales} . '|';
		print OUT $row->{retail_price} . '|';
		print OUT $row->{retail_sales} . '|';
		print OUT $row->{on_hand} . '|';
		print OUT $row->{private_label_flag} . '|';
		print OUT $row->{price_multiple} . '|';
		print OUT $row->{multi_pack_ind} . '|';
		print OUT $row->{selling_channel} . '|';
		print OUT $row->{state} . '|';
		print OUT $row->{country_code} . '|';
		print OUT $row->{site_id} . '|';
		print OUT $row->{zip_code} . '|';
		print OUT $row->{dept_name} . '|';
		print OUT $row->{class_descr} . '|';
		print OUT $row->{sub_class_descr} . '|';
		print OUT $row->{sub_sub_class_descr} . '|';
		print OUT $row->{game_platform} . "\n";
	}

	# Produce trailer record and store in variable for later use
	my $trailerrecord =
	  createTrailerRecord( $merch_year, $merch_period, $merch_week );

	# Write trailer record at tail end of flat file
	print OUT $trailerrecord . "\n";

	# close flat file
	close OUT;
}

#------------------------------------------------------------------------------------
# Create Trailer record to be appended to flat file
#------------------------------------------------------------------------------------
sub createTrailerRecord {
	my $merch_year   = shift;
	my $merch_period = shift;
	my $merch_week   = shift;

	my $getTrailerRecord = <<NPDTRAILERREC;
SELECT	'5625'||substr(week_ending_date,1,2)||substr(week_ending_date,4,2)||substr(week_ending_date,7,4)||'RECORDCOUNT='||rcount||'DOORCOUNT='||scount
FROM		
   	 (SELECT m.week_ending_date, count(*)+1 rcount,count(DISTINCT(site_id)) scount
    	 FROM 		npd_data n, merch_cal_mike_week\@mc2p m
    	 WHERE		n.merchandising_year = m.merchandising_year AND
			n.merchandising_period = m.merchandising_period AND
			n.merchandising_week = m.merchandising_week AND
			n.merchandising_year = ? AND
			n.merchandising_period = ? AND
			n.merchandising_week = ?
    	GROUP BY m.week_ending_date)
NPDTRAILERREC

# Execute SQL above to create a one row record set containing the required trailer record
	my $sth = $g_dbh->prepare($getTrailerRecord);
	$sth->execute( $merch_year, $merch_period, $merch_week );
	my $data = $sth->fetchrow_arrayref();

	# Return the trailer record image to the calling code
	return $data->[0];
}

#------------------------------------------------------------------------------------
# SFTP file to NPD
#------------------------------------------------------------------------------------
sub sftpTONPD {
	#TODO All lines commented except if ststement archiveSentFile(); revert for production.
    #TODO take out the curly bracket after archiveSentFile();  
	# # Define argument list needed by NET:SFTP
	# my %arglist;

	# # Perform steps here ONLY if NPD flat file exists
	 if ( -e DEFAULT_OUTPUT_DIR . $g_npdFile ) {

	# 	# Zip file up before sftp'ing to NPD
	# 	zip DEFAULT_OUTPUT_DIR . $g_npdFile => "$g_file"
	# 	  or fatal_error( 'Zipping step of NPD file '
	# 		  . DEFAULT_OUTPUT_DIR
	# 		  . $g_npdFile
	# 		  . ' has failed. Alet RDI personnel!' );

	# 	# Retrieve destination server and directory
	# 	my $dest     = $g_cfg->npd_DATA->{FTP_SERVER};
	# 	my $remote_d = $g_cfg->npd_DATA->{REMOTE_DIR};

	# 	# Retrieve NPD user name and password
	# 	$arglist{user}     = $g_cfg->npd_DATA->{USER};
	# 	$arglist{password} = $g_cfg->npd_DATA->{PSWD};

	# 	# Log server name and directory
	# 	$g_log->info("FTP_SERVER: $dest");
	# 	$g_log->info("REMOTE_DIR: $remote_d");

	# 	# Establish SFTP connection to NPD server
	# 	my $sftp;
	# 	my $num_retry      = 10;
	# 	my $successful_ftp = 'N';
	# 	my $sftp_error;
	# 	my $attempt;
	# 	while ( $num_retry-- ) {
	# 		eval { $sftp = Net::SFTP::Foreign->new( $dest, %arglist ) };
	# 		if ( !$@ ) { $successful_ftp = 'Y'; last }
	# 		$attempt = 10 - $num_retry;
	# 		$g_log->info("Attempt $attempt to connect to $dest failed!\n");
	# 		sleep(10);
	# 	}

	# 	if ( $successful_ftp eq 'N' ) {
	# 		fatal_error("SFTP connection to NPD server ($dest) failed!");
	# 	}
	# 	$sftp->put( $g_file, $remote_d . basename($g_file),copy_time => 0, copy_perm => 0 );
	# 	$sftp_error = $sftp->status;
	# 	if ($sftp_error) {
	# 		fatal_error(
	# 			"SFTP PUT of basename($g_file) to NPD server ($dest) failed!");
	# 	}
	# }
	# else {
	# 	fatal_error( 'NPD flat file '
	# 		  . DEFAULT_OUTPUT_DIR
	# 		  . $g_npdFile
	# 		  . ' does not exist. Alert RDI personnel!' );
	# }

	# # Archive succesfully sent file

	archiveSentFile();}
}

#------------------------------------------------------------------------------------
#  Routine to archive produced and sent files
#------------------------------------------------------------------------------------
sub archiveSentFile {

	# Retrieve name of NPD Archive directory
	my $arc_dir = $g_cfg->npd_DATA->{ARC_DIR};

	# Create archive directory
	if ( !-d "$arc_dir/$g_date" ) {
		mkpath("$arc_dir/$g_date")
		  or fatal_error("Creation of directory $arc_dir/$g_date has failed!");
	}

	# Archive npd file
	copy( DEFAULT_OUTPUT_DIR . $g_npdFile, "$arc_dir/$g_date/$g_npdFile" )
	  or fatal_error(
		"Archiving of " . DEFAULT_OUTPUT_DIR . $g_npdFile . ' failed!' );

	# Archive zip file
	copy( $g_file, "$arc_dir/$g_date/" . basename($g_file) )
	  or fatal_error( "Archiving of $arc_dir/$g_date/" . basename($g_file) );

	# Send email confirming that process ended normally
	send_mail( 'NPD EXTRACT',
		' SALES EXTRACT FILE HAS BEEN SUCCESFULLY SENT TO NPD SERVER ' );
}

#------------------------------------------------------------------------------------
#      Routine to send/email errors and croak
#------------------------------------------------------------------------------------

sub fatal_error {
	my $msg = shift;
	send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $msg );
	$g_log->info($msg);
	croak($msg);
}
#------------------------------------------------------------------------------------
#      Routine to send notification emails
#------------------------------------------------------------------------------------
sub send_mail {
	my $msg_sub  = shift;
	my $msg_bod1 = shift;
	my $msg_bod2 = shift || '';
	return if $g_verbose;    # Dont want to send email if on verbose mode

	foreach my $name ( sort keys %{$g_emails} ) {
		$g_log->info( "Sent email to $name (" . $g_emails->{$name} . ")" );
		$g_log->info("  Sbj: $msg_sub ");
		$g_log->debug("  $msg_bod1 ");
		$g_log->debug("  $msg_bod2 ");
		open( MAIL, "|/usr/sbin/sendmail -t" );
		print MAIL "To: " . $g_emails->{$name} . " \n";
		print MAIL "From: rdistaff\@usmc-mccs.org\n";
		print MAIL "Subject: $msg_sub \n";
		print MAIL "\n";
		print MAIL $msg_bod1;
		print MAIL $msg_bod2;
		print MAIL "\n\nServer: " . `hostname` . "\n";
		print MAIL "\n";
		print MAIL "\n";
		close(MAIL);
	}
}

#------------------------------------------------------------------------------------
#  Handle condition where not all sales are in for all commands
#------------------------------------------------------------------------------------
sub HandleSalesNotReady {

# If this is not a retry, simply send email indicating that a reattempt will take place in 12 hours
	if ( $retry ne 'y' ) {
		send_mail(
			'NPD EXTRACT - FIRST ATTEMPT',
			' SALES ARE NOT IN FOR ALL REQUIRED COMMANDS',
			'; AN ATTEMPT WILL BE MADE TO REPROCESS 12 HOURS FROM NOW'
		);
	}
	else {
		send_mail(
			'***URGENT*** NPD EXTRACT - SECOND ATTEMPT ***URGENT***',
			' SALES ARE NOT IN FOR ALL REQUIRED COMMANDS',
'; THIS MUST BE LOOKED INTO IMMEDIATELY AS WE ARE AT RISK OF MISSING THE TIME WINDOW FOR THE REQUIRED NPD PROCESSING'
		);
	}
	exit;
}

#------------------------------------------------------------------------------------
# Check if file has been succesfully sent already
#------------------------------------------------------------------------------------
sub HasFileBeenSent {

	my $g_date = `date -d '1 day ago' +%F`;
	chomp($g_date);

	# Retrieve name of NPD Archive directory
	my $arc_dir = $g_cfg->npd_DATA->{ARC_DIR};

	# Determine if file has beenn sftp'd to NPD before
	if ( -e $arc_dir . '/' . $g_date . '/' . $g_file_to_ftp ) {
		return 1;
	}
	else {
		return 0;
	}

}
#------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------
