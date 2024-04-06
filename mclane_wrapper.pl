#!/usr/local/mccs/perl/bin/perl
#--------------------------------------------------------------------------
#Ported by : Hung Nguyen
#Date      : 12/19/2023
#
#Brief Desc:  Get all files from Mclane into pre_staging directory.
#             Move each file to the staging directory and call processing program.
#             Processing one file at a time. Concatinating all reject files,
#             and success files into two files. Load them into two tables, 
#             then sort them in that table, and print out list of UPCs yuc 10/21/21
#			 
#Mofified by: Kaveh Sari
#	      3/27/2024
#	      Validated this procedure/script and added the GetOptoins 
#	      functionality for consistency.
#	      Changed the System($calling_cmd) statement to use the desired
#	      version of PERL versus the version which is returned by "which Perl".
#	      Since the system command was calling Perl as an executable, the 
#	      fully "pathed" version of this statement will invoke the desired version
#	      of PERL.
# --------------------------------------------------------------------------   

use strict;
use Net::SFTP::Foreign;
use POSIX qw(strftime);
use Getopt::Long;
use IBIS::Log::File;
use IBIS::Log::File;
use MCCS::WMS::Sendmail;
use MCCS::Config;
use Data::Dumper;
use IBIS::RPT2DB;

my $sm = MCCS::WMS::Sendmail->new();
### globle variables
my $debug = 0;
my $date_time = strftime( '%Y_%m_%d_%H_%M',    localtime );
my $day_d     = strftime( '%Y%m%d',    localtime );
my $c_ymd     = strftime( '%Y%m%d',    localtime );
my ($g_cfg, $g_log_dir, $g_log_file, $g_from_dir, $g_pstaging_dir, 
$g_staging_dir, $g_report_email, $g_rpt_dir, $g_arch_dir, $g_reject_config, 
$g_reject_file_pattern, $g_success_config, $g_success_file_pattern, $g_db_name, $reject_report_table, 
$success_report_table, $reject_file_pattern, $success_file_pattern, $g_rpt_stg_dir );

my $options = (GetOptions(
	'debug'      => \$debug,
	)
);

$g_cfg = new MCCS::Config;
$g_log_dir              = $g_cfg->MCLANE_COST->{log_dir};
$g_log_file             = $g_log_dir."mclane_cost_load_log_".$date_time;
$g_from_dir             = $g_cfg->MCLANE_COST->{remote_dir};
$g_pstaging_dir         = $g_cfg->MCLANE_COST->{pre_staging_dir};
$g_staging_dir          = $g_cfg->MCLANE_COST->{staging_dir};
$g_db_name              =  $g_cfg->MCLANE_COST->{db_name};
my $dbh = IBIS::DBI->connect( dbname => $g_db_name, attribs => { AutoCommit=> 0 } );
#$g_report_email         = $g_cfg->MCLANE_COST->{tech_emails};
$g_report_email->{kav}  = 'kaveh.sari@usmc-mccs.org';
$g_rpt_dir              = $g_cfg->MCLANE_COST->{rpt_dir} . $day_d;
unless( -s $g_rpt_dir){
    my $mk_cmd ="mkdir -p $g_rpt_dir";
    system($mk_cmd);
}

###$g_reject_config        = '/usr/local/mccs/etc/mclane_cost/mclane_reject_load.config';
$g_reject_config        = $g_cfg->MCLANE_COST->{reject_config};
###$g_success_config       = '/usr/local/mccs/etc/mclane_cost/mclane_success_load.config';
$g_success_config       = $g_cfg->MCLANE_COST->{success_config};
$g_reject_file_pattern  = 'Summary_reject_report';
####$g_reject_file_pattern  = $g_cfg->MCLANE_COST->{reject_file_pattern};
$g_success_file_pattern = 'sum_success_report';
###$g_success_file_pattern = $g_cfg->MCLANE_COST->{success_file_pattern};
###my $reject_report_table  = 'mclane_rejected_upc_daily';
$reject_report_table  =  $g_cfg->MCLANE_COST->{reject_report_table};
##my $success_report_table = 'mclane_success_upc_daily';
$success_report_table = $g_cfg->MCLANE_COST->{success_report_table};
$g_rpt_stg_dir = $g_cfg->MCLANE_COST->{rpt_stage}; #Changed per LL by KS 04/02/2024
#$g_rpt_stg_dir ='/usr/local/mccs/data/mclane_cost/rpt_stage/'; 


## initiate a log
my $sftp_log = '/usr/local/mccs/log/mclane_cost_load/mclane_wrapper_'.$date_time.'.log';
my $log = IBIS::Log::File->new( {file => $sftp_log} );
$log->info("Process started:\n");
$log->info("$date_time");
my $go_mail = MCCS::WMS::Sendmail->new();

sub log_debug {
	my $log_entry = join( '', "(PID $$) ", @_ );
	$log->info($log_entry);
	debug($log_entry);
}
sub debug {
	if ($debug) {
  	   print "DEBUG: ", @_, "\n";
        }
}

my ($START_MID_WEEK, $END_OF_THE_WEEK,$s_of_the_week, $e_of_the_week, $e_of_last_week, $s_of_last_week);
($s_of_the_week, $e_of_the_week, $e_of_last_week, $s_of_last_week) = &get_week_days_info();
log_debug("$s_of_the_week \n $e_of_the_week \n $e_of_last_week \n $s_of_last_week\n");
### get the start of this week, end of this week, and end_of_last week
### compare with current date, if equal to start date,  or smaller that the end_of_the_week, means  case 1:
### if the current_date is end of the week, create a report.
### case 1: if the start or middle of a week, just run the program to process the day's files
### case 2: if the end of a week, run the process, and create a report from the start_of_the_week 

if (($c_ymd > $s_of_the_week )&&($c_ymd <= $e_of_the_week )){
    $START_MID_WEEK = 1;
    log_debug( "start of middle of the week\n");
}

if ($c_ymd == $s_of_the_week){
    $END_OF_THE_WEEK = 1;
    print "end of the week\n";
}

## only purge the table when it is the end of the week
###$END_OF_THE_WEEK = 1;
if ($END_OF_THE_WEEK){
### purge old summary data:
    my $purge1 ='delete from mclane_rejected_upc_daily';
    my $purge2 ='delete from mclane_success_upc_daily';
    eval{
	$dbh->do($purge1);
	$dbh->do($purge2);
    };
    if ($@){
	$log->info("purge errors:". $@);
    }else{
	$dbh->commit;
	$log->info("purge tables mclane_rejected_upc_daily and mclane_success_upc_daily successsful");
    }
}

###
### fetch files from mclane cost file sftp server
eval{
    &sftpFromMclane($log);
};


if ($@){
    $log->info ("sftpFromMclane error:"."$@");
    my $go_mail = MCCS::WMS::Sendmail->new();
    $go_mail->subject('sftpFromMcLane Error');
    $go_mail->sendTo($g_report_email);
    $go_mail->msg('Failed to fetch mclane cost load file at '.$date_time . " $@ ");
    $go_mail->send_mail();
     ## exit; ## if file fetching failed, should the program stop?
}else{
    $log->info ("sftpFromMclane sccessful");

}

### open dir, print filename: copy file from prestaging to staging. one at a time.
### processing file  one at a time by calling the subprogram, workingxxx

opendir (DIR, $g_pstaging_dir);
    my @dir = readdir DIR;

foreach my $file (@dir) {
    if ($file =~ /^\./g){
	next;
    }
    log_debug("processing file:".$file."\n"); 
    my $cmd = "mv ".$g_pstaging_dir."/".$file."  ".$g_staging_dir."/".$file;
    system($cmd);
    ####my $calling_cmd ="perl /usr/local/mccs/scratch/yuc_temp/mclane_cost/working_mclane_cost_load.pl";
    my $calling_cmd ="perl ./mclane_cost_load.pl --debug $debug";
    eval{
	system($calling_cmd);
    };
    system('/bin/sleep 20');
};

close DIR;

if ($END_OF_THE_WEEK){

    eval {

#### concatinate all reject report into a single file, load into db, and sort, and output file
    my $go_mail = MCCS::WMS::Sendmail->new();
    
#### if the dir is there, means loading process worked and something processed..
    my ($single_file, $reformated_file1, $single_file2, $reformated_file2, $header1, $header2 );
    
    
    if ( -e $g_rpt_stg_dir){
### 
	$single_file = $g_rpt_stg_dir."/".$g_reject_file_pattern."_".$day_d.".rpt";
	$reformated_file1 = $g_rpt_stg_dir."/"."Reformated_reject_report"."_".$day_d.".rpt";
        
	my $cmd_line ="/bin/cat $g_rpt_stg_dir"."/report_mclane_cost_*  ".">> $single_file";
	system($cmd_line);
	
### load into a table for the sake of sorting...
	if (-s $single_file){
	    &load_reject_UPC_files($g_rpt_stg_dir); 
	    
### this list is the REAL FIELD NAMES of these two tables, need exactly...
	    my $header1 = "BUYER|DEPT_ID|DEPT_NAME|RECORD_TYPE|VENDOR_STYLE|DESCRIPTION|UPC_TYPE|MCLANE_UPC|RMS_STYLE|VENDOR|MCL_COST|INNER_PACK|OUTER_PACK|EFFECTIVE_DATE|MCL_GROUP_ID|STORE_ID|REASON\n";
	    my $r_query = &get_query_by_list($reject_report_table,$header1,' order by dept_name,reason, vendor_style asc');
	    log_debug ("$r_query\n");
	    $log->info($r_query);
	    $reformated_file1 = &print_data_by_query_order($dbh, $r_query, '|', $reformated_file1, $header1);
	}
	
	if ( -s $reformated_file1){
	    $log->info("Mclane Sorted Report creation successful.");
	    &send_mail_with_attachment('Mclane Cost Load Reject Report',
				       'The rejected rows of cost are in the attached file.',
				       $reformated_file1);
	}elsif( -s $single_file ){
	    $log->info("Mclane Sorted report not created. send unsorted report.");
	    &send_mail_with_attachment('Mclane Cost Load Reject Report',
				       'The rejected rows of cost are in the attached file.',
				       $single_file);
	}else {
	    $log->info("No Mclane Report is created.");
	}
    }
    
    
    
### concatinate all success report into a single file
    my $go_mail2 = MCCS::WMS::Sendmail->new();
    
## if the dir is there, means loading process worked and something processed..
    my $single_file2;
    if ( -e $g_rpt_stg_dir){
	$single_file2 = $g_rpt_stg_dir."/".$g_success_file_pattern."_".$day_d.".rpt";
	$reformated_file2 = $g_rpt_stg_dir."/"."Reformated_success_report_mclane_".$day_d.".rpt";
	my $cmd_line2 ="/bin/cat $g_rpt_stg_dir"."/success_report_mclane_cost_*  ".">> $single_file2";
	system($cmd_line2);
	
	if ( -s $single_file2){
	    &load_success_UPC_files($g_rpt_stg_dir);
	    my $header2 = "BUYER|DEPT_ID|DEPT_NAME|VENDOR|RECORD_TYPE|VENDOR_STYLE|DESCRIPTION|UPC_TYPE|MCLANE_UPC|RMS_STYLE|STYLE_TYPE|INNER_PACK|OUTER_PACK|EFFECTIVE_DATE|END_DATE|MCL_GROUP_ID|ZONE_ID|STORE_ID|MCL_COST|OLD_COST|COST_VARIANCE|PERCENT_VARIANCE|RTL_PRICE|IMU\n";
	    
	    my $s_query = &get_query_by_list($success_report_table, $header2,' order by dept_name, vendor_style, cost_variance asc');
	    log_debug ("$s_query\n");
	    $log->info($s_query);
	    $reformated_file2 = &print_data_by_query_order($dbh, $s_query, '|', $reformated_file2, $header2);
	}
	
	if ( -s $reformated_file2){
	    $log->info("Mclane Report creation successful.");
	    &send_mail_with_attachment('Mclane Cost Load Success Report',
				       'The UPCs that passed validation are in the attached file.',
				       $reformated_file2);
	    
	} elsif ( -s $single_file2){
	    $log->info("Mclane Report creation successful.");
	    &send_mail_with_attachment('Mclane Cost Load Success Report',
				       'The UPCs that passed validation are in the attached file.',
				       $single_file2);
	} else {
	    $log->info("No Mclane Report is created.");
	}
   
    #### clear the g_rpt_stag_dir
    } ##
    };

    if ($@){
	$log->info("Mclane Reporting process error out:". $@);
    }
    
    my $cp_total_files ="mv -f $g_rpt_stg_dir/*  $g_rpt_dir ";
    my $ret_sys = system($cp_total_files); ### move away all files from the report staging table..
    if ($ret_sys){
	$log->info(" moving files ERRORS".$cp_total_files);
	$log->info($ret_sys);
    }else{
	$log->info("Moving away files success!".$cp_total_files);
    }
    
} ## END OF THE WEEK

$dbh->disconnect;


#################### SUBROUTINES ##############################
sub get_week_days_info{
    my $query = "
       select 
         to_char(TRUNC(sysdate, 'DAY'),'YYYYMMDD') start_of_the_week,
         to_char(TRUNC(sysdate+6, 'DAY')-1,'YYYYMMDD') end_of_the_week,
         to_char(TRUNC(sysdate+6, 'DAY')-8,'YYYYMMDD') end_of_the_last_week,
         to_char(TRUNC(sysdate, 'DAY') - 7,'YYYYMMDD') start_of_the_last_week
       from dual";
    my $sth_days;
    my $data_extraction_error;
    my ($start_of_the_week, $end_of_the_week, $end_of_last_week, $s_of_last_week);

    my $dbh = IBIS::DBI->connect( dbname => $g_db_name);
    eval{
        $sth_days = $dbh->prepare($query);
        $sth_days->execute;
    };

    if ($@){
        my $msg = "get_week_days_info error:".$@ ;
        ##$log->info($msg);
	log_debug ("$msg\n");
        $data_extraction_error = 1;
    }else{
        my $msg = "get_week_days_info success.";
	log_debug("$msg\n");
        ##$log->info($msg);
    }

    while (my $r = $sth_days->fetchrow_hashref) {
        $start_of_the_week  = $r->{'start_of_the_week'};
        $end_of_the_week    = $r->{'end_of_the_week'};
	$end_of_last_week   = $r->{'end_of_the_last_week'};
	$s_of_last_week     = $r->{'start_of_the_last_week'};
    }
    $dbh->disconnect;
    return ($start_of_the_week, $end_of_the_week, $end_of_last_week, $s_of_last_week);
}


sub print_data_by_query_order{
## input: query, delimiter
## output: filename of the printed out data
    my ($dbh, $query, $delim, $outfile, $header) = @_;
    my $ret;
    eval{
        $ret = $dbh->selectall_arrayref($query);
    };
    if ($@){
        my $msg = "DB error in selection".$@ ;
        log_debug ($msg);
    }

    log_debug(Dumper($ret));
    ### write the content $ret into a file ...
    if (@{$ret} > 0){
        open(OUT, ">$outfile") or die "failed to open file to write:".$outfile;
	print OUT $header;
	for ( my $i = 0 ; $i < @$ret ; $i++ ) { ### @$ret: how many rows
            for ( my $j = 0 ; $j < @{ $ret->[$i] } ; $j++ ) { 
            ### @{$ret->[$i]: how many columns on ### row $i
               print OUT "$ret->[$i][$j]"."|";
            }
            print OUT "\n";
        }
        close OUT;
    }
    return $outfile;
}

## input: tablename, field_list, order_by clause
## output: a complte query
sub get_query_by_list{
    my ($tn, $flist, $ob) = @_;
    my $query ='';
    $query .= 'select ';
    $flist =~ s/\|/,/g; #### replace all | with comma
    $query .= $flist;
    $query .= ' from '.$tn.' ';
    $query .= $ob;
    return $query;
}


sub load_reject_UPC_files{
    my ($reject_data_dir) = @_;
    my $wms = IBIS::RPT2DB->new( conf_file => $g_reject_config );
    $wms->{'dbh_obj'} = IBIS::DBI->connect(
        dbname  => $g_db_name,
        attribs => { AutoCommit => 0 }
        ) or die "failed to connect db";
    $wms->load_a_report_table($reject_data_dir, $g_reject_file_pattern, '|','');
}

sub load_success_UPC_files{
    my ($success_data_dir) = @_;
    my $wms = IBIS::RPT2DB->new( conf_file => $g_success_config );
    $wms->{'dbh_obj'} = IBIS::DBI->connect(
        dbname  => $g_db_name,
        attribs => { AutoCommit => 0 }
        ) or die "failed to connect db";
    $wms->load_a_report_table($success_data_dir, $g_success_file_pattern, '|','');
}


sub send_mail_with_attachment {
    my $subject = shift;
    my $body = shift;
    my $file = shift;

    my @emails = values( %{$g_report_email} );
    $log->info("Sending attachment to:");
    foreach my $e ( sort keys %{$g_report_email} ) {
        $log->info(" $e ($g_report_email->{$e})");
    }
    $log->info(" mail_report");
    $log->info(" Subject: $subject");
    $log->info(" Attachment: $file") if $file;

    $go_mail->logObj($log);
    $go_mail->subject($subject);
    $go_mail->sendTo($g_report_email);
    $go_mail->attachments($file) if $file;
    $go_mail->msg($body);

    if ($file) {
        $go_mail->send_mail_attachment();
    }
    else {
        $go_mail->send_mail('No mclane cost load file');
    }
}

### Fetch cost file from Mclane
### note, using mcl_DATA config info

sub sftpFromMclane {
    my ($log) = @_;
   # Instantiate MCCS::Config object
    my $g_cfg = new MCCS::Config;
    # Define argument list needed by NET:SFTP
    my %arglist;

    # Retrieve destination server and directory
    my $dest     = $g_cfg->mcl_DATA->{FTP_SERVER};
    # Retrieve MCL user name and password
    $arglist{user}     = $g_cfg->mcl_DATA->{USER};
    $arglist{password} = $g_cfg->mcl_DATA->{PSWD};
    $arglist{more} = '-v';
    # Log server name and directory
    $log->info('SFTP transfer started' );
    $log->info("FTP_SERVER: $dest");

    # Establish SFTP connection to MCL server
    my $sftp;
    my $num_retry      = 10;
    my $successful_ftp = 'N';
    my $attempt;

    while ( $num_retry-- ) {
    log_debug("$num_retry eval sftp connection");
    eval { $sftp = Net::SFTP::Foreign->new( $dest, %arglist ) };
    if ( !$@ ) { $successful_ftp = 'Y'; last }
    $attempt = 10 - $num_retry;
    $log->info("Attempt $attempt to connect to $dest failed!\n");
    sleep(10);
    }

    if ( $successful_ftp eq 'N' ) {
       $log->info("SFTP connection to Mclane server ($dest) failed!");
        die;
     }

    my $cur_dir = $sftp->setcwd($g_from_dir);
    log_debug(Dumper($cur_dir));
    my $list_file = $sftp->ls;
    ####print Dumper($list_file) if $debug;
    eval{
	foreach my $ary_ele(@{$list_file}) {
	    my $filename = $ary_ele->{'filename'};
	    if ($filename =~ /^MCX/g){
			my $from_file = $g_from_dir.'/'.$filename;
			my $dest_file = $g_pstaging_dir.  '/'.$filename;
			my $result = $sftp->get($from_file, $dest_file);
                	log_debug("from $from_file to $dest_file");
	    }
	}
    };

    if ($@){
	log_debug ("error:".$@);
	$log->info("SFTP Error:".$@);
    }else{
	log_debug("sftp successful");
	$log->info("SFTP successful");
    }
    ### this remove is not necessary, I found mclane sftp server
    ### will delete any file that has
    ### been fetched, or accessed by the sftp program.
    ### $sftp->remove($g_from_dir, wanted => qr/MCX_COSTUPD/)
       ### or die "remove remote files failed :".$sftp->error;
}

#----------------------------------------------------------------#
# Routines for Random all Other needed Stuff-Logs,Emails,Markers
#----------------------------------------------------------------#
