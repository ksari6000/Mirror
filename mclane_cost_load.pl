#!/usr/local/mccs/perl/bin/perl
use strict;
use IBIS::DBI;
use DateTime;
use Data::Dumper;
use Net::SFTP::Foreign;
use MCCS::Config;
use Net::SMTP;
use Carp;
use Getopt::Long;
use IBIS::Log::File;
use File::Basename;
use File::Spec;
use File::Copy;
use File::Path;
use IBIS::RPT2DB;
use POSIX qw(strftime);
use MCCS::WMS::Sendmail;

my $debug = 0; ## Note, if value 1, will not insert iri table, if 0, will insert data
my $verbose = 1;
my $date_time = strftime( '%Y_%m_%d_%H_%M_%S',    localtime );
my $day       = strftime( '%Y%m%d',    localtime );
my $c_yyyymmdd  = strftime( '%Y%m%d',    localtime );
my (
    ## variables for caching db data
    $cache_upc,
    $cache_style,
    $cache_iscurrent,
    $cache_cost,
    $cache_ship_cost,
    $cache_pack_cost,
    $comb_pack_and_ship,
    ## each data set will be saved under different statement handle
    $sth_upc, 
    $sth_style_set, 
    $sth_data_current, 
    $sth_cost_unloaded,
    ## variables needed for config, including data directory, and log
    $g_cfg,
    $g_to_dir, 
    $g_log_dir,
    $g_config_file, 
    $g_file_pattern, 
    $g_db_name, 
    $g_log_file, 
    $g_data_dir, 
    $g_archive_dir,
    $g_from_dir,
    $g_report_email,
    $g_rpt_dir,
    $g_rpt_stg_dir,
    ## any other variables
    $data_extraction_error,
    $found_files,
    $max_ctr,
    $added_hash,
    $s_filename
    );

## get config value from config file
$g_cfg = new MCCS::Config;
$g_config_file     = $g_cfg->MCLANE_COST->{config_file};  ### required!!!
$g_file_pattern    = $g_cfg->MCLANE_COST->{file_pattern};
$g_db_name         = $g_cfg->MCLANE_COST->{db_name};
$g_log_dir         = $g_cfg->MCLANE_COST->{log_dir};
$g_log_file        = $g_log_dir."mclane_cost_load_log_".$date_time;
$g_from_dir        = $g_cfg->MCLANE_COST->{remote_dir};
### note: $g_from_dir = '/CMI/COSTUPD'; ## default to /CMI/ for sftp
$g_data_dir        = $g_cfg->MCLANE_COST->{data_dir};
$g_to_dir          = $g_cfg->MCLANE_COST->{to_dir};
$g_rpt_dir         = $g_cfg->MCLANE_COST->{rpt_dir};
#$g_rpt_stg_dir     ='/usr/local/mccs/data/mclane_cost/rpt_stage/'; #Changed
$g_rpt_stg_dir     =$g_cfg->MCLANE_COST->{rpt_stage};               #change
$g_archive_dir     = $g_cfg->MCLANE_COST->{archive_dir};
#$g_report_email    = $g_cfg->MCLANE_COST->{tech_emails};
$g_report_email->{kav}  = 'kaveh.sari@usmc-mccs.org';
my $options = (GetOptions(  #ChangekS / Add
	'debug'      => \$debug,
	)
);

sub log_debug {
	my $log_entry = join( '', "(PID $$) ", @_ ); #changeKS / Add
	$log->info ($log_entry);
	debug($log_entry);
}

sub debug {                                      #changeKS / Add
	if ($debug) {
  	   print "DEBUG: ", @_, "\n";
        }
}

## initiate a log
my $log = IBIS::Log::File->new( {file => $g_log_file} );
&log_debug ("Process started:\n");              #ChangeKS ALL $log->info to &Log_debug
&log_debug ("$date_time");
my $go_mail = MCCS::WMS::Sendmail->new();

if ($debug){
    my $values_of_config = qq(
Config values\n
g_to_dir:         $g_to_dir         
g_config_file:    $g_config_file     
g_file_pattern:   $g_file_pattern   
g_db_name:        $g_db_name        
g_log_file:       $g_log_file        
g_data_dir:       $g_data_dir       
g_from_dir:       $g_from_dir        
g_report_email:   $g_report_email 
);
    &log_debug ($values_of_config);
}

## values in the config file need to be decided in production:
##my $to_dir          = '/usr/local/mccs/data/mclane_cost/staging/';
##my $g_config_file   = '/usr/local/mccs/scratch/yuc_temp/mclane_cost/mclane_cost.cfg';
##my $g_data_dir      = '/usr/local/mccs/scratch/yuc_temp/mclane_cost/staging';
##my $g_file_pattern  = 'MCX_COST';
##my $g_db_name       = 'rms_p';
##my $g_log_file      = '/usr/local/mccs/log/mclane_cost_load.log';


########################### MAIN ################################

## 
## purge mclane_cost_input_arch older than 60 days
&purge_arch_over_60_days();

### move away last mclane_cost_input to mclane_cost_input_arch table,clearing the staging table (per process)
&archive_and_purge_last_cost();

## clear mclane_upc_status  to mclane_upc_status_arch table
&purge_status_data();


## if new file fetchd into the staging dir, load to mclane_cost_input table
## if no new files at all, exit. 
($found_files, $s_filename) =  &checkFilesFetched ($g_to_dir, $g_file_pattern);
unless($found_files){
    &log_debug ("No new mclane cost files from the sftp step. Program will exit.");
    &send_mail_with_attachment('No New Cost from Mclane','Program exit as no new data','');  
    exit;
}else{
    &log_debug ("New files fetched from Mclane:".$found_files);
    eval{
	&loadFetchedFiles($g_config_file, $g_data_dir,$g_file_pattern);
    };
    if ($@){
	&log_debug ("Loading Mclane Cost data errors.".$@);
	&send_mail_with_attachment('Mclane Cost loading error','Program exit with loading error.',''); 
    }else{
	&log_debug ("Mclane cost data load sucess in mclane_cost_input table.");
    }
}


### start the real thing from here:

### 1, Verify if packupc, and shipupc from mclane exists in RMS bar_code table:
$data_extraction_error = 0; ## a global variable to keep track if there is extraction errors
## get upc list of the mclane input where its ship or pack upc are in RMS 
$cache_upc = &get_upc_in_rms();
print Dumper($cache_upc) if $debug;
$cache_upc = &check_upc($cache_upc); ### upc in rms as input, marking ones not in RMS in ret
print Dumper($cache_upc) if $debug ;


### 2, Verify styles associated to the shipupc, and packupc in the file in RMS style_vendor table
$cache_style = &get_style_set();
print Dumper($cache_style) if $debug;
$cache_upc = &check_style_setup($cache_upc, $cache_style);
print Dumper($cache_upc)  if $debug ;


### 3, Verify effective date on the record is current, or in the future
$cache_iscurrent = &get_date_is_current();
print Dumper($cache_iscurrent) if $debug ;
$cache_upc = &check_date_is_current($cache_upc, $cache_iscurrent);
print Dumper($cache_upc)  if $debug;
### Note: the above code is just for the sake of getting a report as required in spec.
### not the best practice in terms of coding

### extracting data from a union of all cases of upc needed cost update including: 
### oneliner per packupc (pack and ship both),  multiliner per packupc 
### (for multi, use shipupc only, as single packupc in multi are as shipupc in mclane file).
### This union view has anchor upc, anchor price, style_id for loading
### here is the logic to decide anchor_upc, and anchor_price, 
### style_id matches only to anchor_upc
### if upc_type is ship, then anchor upc is shipupc
### if upc_type is pack, then anchor upc is packupc
### if style_type is single, or regular, then, unit cost is calculated cost/innerXouter
### for this upc, no matter ship or pack type...
### if style_type is multi, then, cost  is used directly for the whole shipupc.

my $valid_styles_to_load = &extract_style_cost_from_union($log);
&log_debug (Dumper($valid_styles_to_load))  if $debug;
### NOTE, this return has the valid list of UPCs of the union

### 11, Load style_id, cost, inner_pack_qty, outer_pack_qty into warehouse input table
eval{
    ($valid_styles_to_load, $cache_upc) = &collect_data_for_iri_whsle($valid_styles_to_load, $log, $cache_upc);
};

### email if insertion to iri_whsle_xx table error out:
if ($@){
    &log_debug ("Mclane Insert IRI table failed.");
    my ($subject, $body);
    $subject ='Mclane Cost Data Update to IRI table Error';
    $body    ='Insertion data errors. See mclane_cost_load.log for details.';
    &send_mail_with_attachment($subject, $body,'');
    exit;
}else{
    &log_debug ("Mclane Insert IRI table success.");
}

### 12, Create a report for rejected UPCs, and loaded UPCs 
my $day_dir = $g_rpt_dir."/".$day;
unless ( -e $day_dir){
    my $mk_cmd = "/bin/mkdir $day_dir";
    system($mk_cmd);
}

### this is for rejected UPCs
my $report_file = $day_dir.'/'."report_mclane_cost_".$date_time.'.rpt';
&print_report_file($cache_upc, $report_file, $s_filename);

### this is for valid UPCs
my $success_report_file = $day_dir.'/'."success_report_mclane_cost_".$date_time.'.rpt';
&print_success_report_file($valid_styles_to_load, $success_report_file, $cache_upc, $log);


if ( -s $report_file){
    &log_debug ("Mclane Report creation successful.");
    if ($verbose){
	my $r_subject ='Mclane Cost Input Reject Report for individual cost file';
	my $r_body ='Reject report attached.';
	&send_mail_with_attachment($r_subject, $r_body, $report_file);
    }
### copy the report file to the report staging directory:
    my $cp_cmd = "/bin/cp  ".$report_file. "  ".$g_rpt_stg_dir;
    system($cp_cmd);
} else {
    &log_debug ("No Mclane Reject Report is created.");
}

if ( -s $success_report_file){
    &log_debug ("Mclane Success Report generated.");
    if ($verbose){
	my $s_subject ='Mclane Cost Input Success Report for individual cost file';
	my $s_body ='Success report attached.';
	&send_mail_with_attachment($s_subject, $s_body, $success_report_file);
    }
### copy the report file to the report staging directory:
    my $cp_cmd = "/bin/cp  ".$success_report_file. "  ".$g_rpt_stg_dir;
    system($cp_cmd);
} else {
    &log_debug ("No Mclane Success Report is created.");
}


### 13, move away processed files from staging directory to archive directory
if ($found_files){
    &archive_loaded_files($day);
}

### 14, recording ending time for trouble shooting purpose.
&log_debug ("Process ended at:\n");
my $end_time = strftime( '%Y_%m_%d_%H_%M',    localtime );
&log_debug ("$end_time");


#################### SUBROUTINES ######################################


sub purge_status_data{
## Connect to DB
    my $dbh = IBIS::DBI->connect( dbname => $g_db_name, attribs => { AutoCommit => 0 } );
## copy data to archive table:
    my $q_purge = qq (
insert into mclane_upc_rejected_arch 
select * from mclane_upc_rejected
    );
    my ($sth1, $sth2);
    eval{
        $sth1 = $dbh->prepare($q_purge);
        $sth1->execute;
    };
    if ($@){
        my $msg = "DB error in insert".$@ ;
        &log_debug ($msg);
    }else{
        my $msg = "stauts copy successful.";
        &log_debug ($msg);
        $dbh->commit;
    }

### purge
    my $q_purge2 = qq (
      delete from mclane_upc_rejected
    );
    eval{
        $sth2 = $dbh->prepare($q_purge2);
        $sth2->execute;
    };
    if ($@){
        my $msg = "DB error in purging".$@ ;
        &log_debug ($msg);
    }else{
        my $msg = "Purge status successful.";
        &log_debug ($msg);
        $dbh->commit;
    }
    $dbh->disconnect;

}


sub update_status{
    my ($dbh, $packupc, $shipupc, $reject_ind, $reject_reason) = @_;
    my $query = "insert into mclane_upc_rejected (
        upc, reject_ind, reject_reason,create_date ) values ( 
       \'$packupc\',\'$reject_ind\', \'$reject_reason\', sysdate )";
    
    eval{
	$dbh->do($query);
    };
    my $message;
    my $error_ind;
    if ($@){
	print "failed to insert data".$query;
	$error_ind = 1;
    }else{
	## $dbh->commit;
	$error_ind = 0;
    }
    return $error_ind;
}

### appending counter based hashes together
sub append_hashes{
    my ($h_ref1, $h_ref2) = @_;
## get the largest counter from the first hash
    my $max_ctr1 = 0;
    for my $key (keys %$h_ref1){
        if ($key > $max_ctr1){
            $max_ctr1 = $key;
        }
    }
    print "largest counter of the first hash:$max_ctr1\n";

### loop through the second hash and adding to the tail of the first
    for my $key2 (sort {$a<=>$b} keys %$h_ref2){
        if ($h_ref2->{$key2}){
            $max_ctr1 ++;
            $h_ref1->{$max_ctr1} = $h_ref2->{$key2};
        }
    }
    return ($h_ref1, $max_ctr1);
}

sub check_status_by_packupc {
    my ($dbh, $packupc) = @_;
    my $query =" select reject_ind from mclane_upc_rejected where upc = $packupc ";
    my $ret = $dbh->selectall_arrayref($query);
    if ( $ret->[0][0] ) {
        return $ret->[0][0];
    }
    else {
	return 0;
    }
}
### loop through all validat styles, if the one in testing is in there, return valid as true.
sub single_style_validation{
    my ($style_id, $cache_style) = @_;

    my $not_valid = 1; ## assume it is not valid in the begining:
    foreach my $style_ctr (keys %{$cache_style}){
        if($cache_style->{$style_ctr}->{'style_id'} eq $style_id){
            $not_valid = 0; ## found it is valid
        }
    }
    return $not_valid;
}

sub check_upc{
    my ($upc_with_style) = @_;
    my $dbh = IBIS::DBI->connect( dbname => $g_db_name);

    my $query = qq(
     select 
        *
     from 
        v_mclane_pack_and_ship
     );    

    eval{
	$sth_upc = $dbh->prepare($query);
	$sth_upc->execute;
    };

    if ($@){
	my $msg = "UPC data excution error:".$@ ;
	&log_debug ($msg);
    }else{
	my $msg = "UPC extraction success.";
	&log_debug ($msg);
    }

    my $total_upc;
    my $ctr_upc = 0; ## set begining counter as 0
    while (my $r = $sth_upc->fetchrow_hashref) {
	$total_upc->{$ctr_upc}->{'shipupc'}            = $r->{'shipupc'};
	$total_upc->{$ctr_upc}->{'packupc'}            = $r->{'packupc'};
	$total_upc->{$ctr_upc}->{'anchorupc'}          = $r->{'anchorupc'};
        $total_upc->{$ctr_upc}->{'dcs'}                = $r->{'dcs'};
        $total_upc->{$ctr_upc}->{'style_id'}           = $r->{'style_id'};	
        $total_upc->{$ctr_upc}->{'style_type'}         = $r->{'style_type'};
	$total_upc->{$ctr_upc}->{'store'}              = $r->{'store'};
	$total_upc->{$ctr_upc}->{'vendpcode'}          = $r->{'vendpcode'};
	$total_upc->{$ctr_upc}->{'vendpdesc'}          = $r->{'vendpdesc'};
	$total_upc->{$ctr_upc}->{'record_type'}        = $r->{'record_type'};
	$total_upc->{$ctr_upc}->{'retail'}             = $r->{'retail'};
	$total_upc->{$ctr_upc}->{'cost'}               = $r->{'cost'};
	$total_upc->{$ctr_upc}->{'inner_pack_qty'}     = $r->{'inner_pack_qty'};
	$total_upc->{$ctr_upc}->{'outer_pack_qty'}     = $r->{'outer_pack_qty'};
	$total_upc->{$ctr_upc}->{'effective_day'}      = $r->{'effective_day'};
	$total_upc->{$ctr_upc}->{'group_id'}           = $r->{'group_id'};
	$total_upc->{$ctr_upc}->{'upctype'}            = $r->{'upctype'};
        $total_upc->{$ctr_upc}->{'reject_reasons'}     = '';
	$total_upc->{$ctr_upc}->{'vendor_id'}          ='00009830555';
	$ctr_upc++;
    }

    foreach my $ctr_upc(keys %{$total_upc}) {
        if ($total_upc->{$ctr_upc}->{'rejected_ind'}){
            next;
        }else{
            my $found = 0;
	    foreach my $w_ctr_upc(keys %{$upc_with_style}){
		if (
($total_upc->{$ctr_upc}->{'shipupc'} eq $upc_with_style->{$w_ctr_upc}->{'shipupc'}) && 
($total_upc->{$ctr_upc}->{'packupc'} eq $upc_with_style->{$w_ctr_upc}->{'packupc'}) &&
($total_upc->{$ctr_upc}->{'upctype'} eq $upc_with_style->{$w_ctr_upc}->{'upctype'}) &&
( length($upc_with_style->{$w_ctr_upc}->{'style_id'}) > 0)
                    ){
                   $found = 1;
                   $total_upc->{$ctr_upc}->{'rejected_ind'} = 0;
                   $total_upc->{$ctr_upc}->{'style_id'}    = $upc_with_style->{$w_ctr_upc}->{'style_id'};
                   $total_upc->{$ctr_upc}->{'style_type'}  = $upc_with_style->{$w_ctr_upc}->{'style_type'};
		   $total_upc->{$ctr_upc}->{'dcs'}         = $upc_with_style->{$w_ctr_upc}->{'dcs'};
		     }
	    } ## end of innter loop
            unless ($found){
		$total_upc->{$ctr_upc}->{'reject_reasons'}   .=  "UPC not in RMS";
		$total_upc->{$ctr_upc}->{'rejected_ind'}      =  1;
		&update_status($dbh, $total_upc->{$ctr_upc}->{'anchorupc'}, '', '1', 'UPC not in RMS');
	    }
	}## end if total_upc->{rejected_ind}
    }## end outer loop foreach

    $dbh->disconnect;
    return $total_upc; ### here the rejected ones are marked..
}



sub loadFetchedFiles{
      my $wms = IBIS::RPT2DB->new( conf_file => $g_config_file );
      $wms->{'dbh_obj'} = IBIS::DBI->connect(
	dbname  => $g_db_name,
	attribs => { AutoCommit => 0 }
	) or die "failed to connect db";
     $wms->load_a_report_table($g_data_dir, $g_file_pattern, ',','');
}


### check the fetched file in the temp directories
### also, need to add a 'N' as not processed yet
sub checkFilesFetched{
    my ($to_dir, $filepattern) = @_;
    my $found = 0;
    my $f_file  = ''; ## found file name
    my @found_files =  glob ("$to_dir/$filepattern*");
    if (@found_files > 0){
	$found = @found_files;
	foreach my $file (@found_files){
	    &log_debug ("Modifying file:".$file."\n");
	    unless ($f_file){ $f_file = $file;}
	    &add_status_for_cost_file($file);
	}
    }
    return ($found, $f_file);
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

    # Log server name and directory
    &log_debug ('SFTP transfer started' );
    &log_debug ("FTP_SERVER: $dest");
    
    # Establish SFTP connection to MCL server
    my $sftp;
    my $num_retry      = 10;
    my $successful_ftp = 'N';
    my $attempt;
    while ( $num_retry-- ) {
	eval { $sftp = Net::SFTP::Foreign->new( $dest, %arglist ) };
	if ( !$@ ) { $successful_ftp = 'Y'; last }
	$attempt = 10 - $num_retry;
	&log_debug ("Attempt $attempt to connect to $dest failed!\n");
	sleep(10);
    }
    
    if ( $successful_ftp eq 'N' ) {
	&log_debug ("SFTP connection to Mclane server ($dest) failed!");
	die;
    }

    my $cur_dir = $sftp->setcwd($g_from_dir);
    print Dumper($cur_dir) if $debug;
    my $list_file = $sftp->ls;
    print Dumper($list_file) if $debug;
    eval{
	foreach my $ary_ele(@{$list_file}) {
	    my $filename = $ary_ele->{'filename'};
	    if ($filename =~ /^MCX/g){
		my $from_file = $g_from_dir.'/'.$filename;
		my $dest_file = $g_to_dir.  '/'.$filename;
		my $result = $sftp->get($from_file, $dest_file);
	    }
	}
    };
    
    if ($@){
	print "error:".$@;
	&log_debug ("SFTP Error:".$@);
    }else{
	print "sftp successful";
	&log_debug ("SFTP succwssful");
    }
    ### this remove is not necessary, I found mclane sftp server 
    ### will delete any file that has
    ### been fetched, or accessed by the sftp program.
    #$sftp->remove($g_from_dir, wanted => qr/MCX_COSTUPD/) 
       ##or die "remove remote files failed :".$sftp->error; 
}

### Load a inbound rms table
sub insert_iri_whsle_style_vendors {
    my ( $dbh, $h, $log) = @_;

    my $insert_sql = q{
        INSERT  /*+ APPEND */ 
        INTO IRI_WHSLE_STYLE_VENDORS
        VALUES(
         ?,    ?,    ?,    ?,    ?, 
         ?,    NULL, to_date( ?, 'YYYYMMDD'),   to_date( ?,'YYYYMMDD'),    NULL, 
         ?,    NULL, NULL, NULL, NULL, 
         NULL, ?,    ?,    ?,    ?, 
         NULL, ?,    ?,    NULL, NULL, 
         NULL, NULL, NULL, NULL, ?, 
         ?,    SYSDATE,    ?,    ?,    ?, 
         NULL, NULL, NULL, ?,   'O', 
         'O',  NULL, NULL, NULL, NULL, 
         NULL, NULL, NULL, NULL
         )
    };
    
    my $sth = $dbh->prepare_cached($insert_sql);
    &log_debug ("Insert values:".Dumper($h));  ## log every value here:
    
    # There is a unique key constraint on this table. We may see records
    # where the same style is mapped to multiple UPCs. This will cause an
    # insert failure.
    #
    eval {
     ### insert data only when not debug mode:
	##unless ($debug){
        $sth->execute(
            $h->{business_unit_id},
            $h->{job_id},
            $h->{style_id},
            $h->{vendor_id},
            $h->{region_district_sub_type},
            $h->{site_group_id},
            $h->{start_date},
            $h->{end_date},
            $h->{primary_vendor_ind},
            $h->{local_first_cost},
            $h->{cost_descriptor},
            $h->{cost_factor},
            $h->{landed_cost_id},
            $h->{'inner_pack_qty'},    #inner_pack_qty
            $h->{'outer_pack_qty'},    #outer_pack_qty
            $h->{exclude_vendor_rebate},
            $h->{rtv_allowed_ind},
            $h->{created_by},
            $h->{status},
            $h->{process_status},
	    $h->{vendor_style_no}
	    );
	##}
    }
    or &log_debug (
	" dumped data:".Dumper($h)
	);
    
    if ($@) {
	&log_debug ("$@");
    }else{
	$dbh->commit; 
    }
} ## end sub update_iri_whsle_style_vendors


## test if elemenet is in an array
sub check_array{
    my ($array_ref, $the_one) = @_;
    my $found = 0;
    foreach my $element(@{$array_ref}){
        if (($element eq $the_one) or ($element == $the_one)){
            $found = 1;
        }
    }
    return $found;
}

## Each group has multiple prize zones.(like PZ0118).
## Loop each PZ (price zone) from the groupid2zonesite table 
## for each group_id, also, add site id in addition to each zone
## this is what Mike T required. a bit strange. as that seems redundant for PZs.
##399121,MARL SPEC SLCT RED BX FSC,028200156501,028200005656,ADD,,,0,49.24,0,,,20200710,SZ700116,
##399121,MARL SPEC SLCT RED BX FSC,028200156501,028200005656,ADD,,,0,49.24,0,,,20200710,SZ700116,010323

sub collect_data_for_iri_whsle{
    my ($cache_upc, $log, $total_upc) = @_;
    
    my $outfile = '/usr/local/mccs/data/mclane_cost/staging/debug_output.txt';
    open( OUT, ">>$outfile") or die "$!\n";
    
    my $dbh = IBIS::DBI->connect(
	    dbname  => $g_db_name,
	    attribs => { AutoCommit => 0 }
	    ) or die "failed to connect db";
    
    ## get groupid and its zoneid, and site
    my $cache_group = &get_groupid_map();


    foreach my $ctr_upc(keys %{$cache_upc}) {  ### #1
	my $region ='';
	## assign the cache_gorup to a tmp vaiable in case it need to push a store into it and preserver the original value of cached groups:
	my $tmp_cache_group = $cache_group;
	## save this store into the list of zones pointed by this group id:
	if ($cache_upc->{$ctr_upc}->{'group_id'}){
	    ## if there is a store from mclane, add it to the list of zone under this group if not there yet.
	    if ($cache_upc->{$ctr_upc}->{'store'}) {
		### get the last 3 chars of the group_id from Mclane
		my $s_group = substr(
		    $cache_upc->{$ctr_upc}->{'group_id'},
		    length($cache_upc->{$ctr_upc}->{'group_id'}) - 3,
		    3
		    );
		unless (check_array(
			    \@{$tmp_cache_group->{$s_group}}, 
			    $cache_upc->{$ctr_upc}->{'store'}."|".'SITE' 
			)
		    ){
		    push(@{$tmp_cache_group->{$s_group}}, 
			 $cache_upc->{$ctr_upc}->{'store'}."|".'SITE');
		}
	    }
	}
	
	### Expanding each UPC line in the mclane cost file into multiple zone_ids under this group_id
	my $valid_group = 0;
	my $zone_id ='';
	foreach my $cur_group (keys %{$tmp_cache_group}){
	    ## get last 3 letters of the group_id from mclane cost input
	    my $cached_group = substr(
		$cache_upc->{$ctr_upc}->{'group_id'},
		length($cache_upc->{$ctr_upc}->{'group_id'}) -3,
		3);
	    ## if this group looped in the temp cached groups, matches with the UPC's group
	    if($cur_group eq $cached_group){
		$valid_group = $cur_group;
		### expanding each upc to all the zones under this group_id:    
		foreach my $zone_id_and_type (@{$tmp_cache_group->{$cur_group}}){
		    
		    ### if group is valid, populate each zone_id for this group,
		    ### for both ADD and DELETE
		    my ($zone_id, $region) = split(/\|/, $zone_id_and_type);
		    ## skip the line of upc without store value.	
		    if (($cache_upc->{$ctr_upc}->{'store'} ne $zone_id ) && ($region eq 'SITE')){
			next;
		    }
		    unless ($cache_upc->{$ctr_upc}->{'vendor_id'}){
			$cache_upc->{$ctr_upc}->{'vendor_id'} ='00009830555';
		    }
		    
		    if (($cache_upc->{$ctr_upc}->{'record_type'} eq 'ADD')
                         or ($cache_upc->{$ctr_upc}->{'record_type'} eq 'CHG'))
                          {   
                            ### checking on effective_date for CHG type here		
			      my $date_to_load = $cache_upc->{$ctr_upc}->{'effective_day'};
			      
			      if ( ($cache_upc->{$ctr_upc}->{'record_type'} eq 'CHG') 
				   && ( $date_to_load eq $c_yyyymmdd)){
				  $date_to_load = &get_tomorrow_date_string;
			      }
			      			
			&insert_iri_whsle_style_vendors(
			     $dbh, 
			     {
				 business_unit_id         => '30',
				 job_id                   => '999999999999',
				 style_id                 => $cache_upc->{$ctr_upc}->{'style_id'},
				 vendor_id                => $cache_upc->{$ctr_upc}->{'vendor_id'},
				 region_district_sub_type => $region,
				 site_group_id            => substr($zone_id, length($zone_id) - 5, 5),
				 start_date               => $date_to_load,
				 end_date                 => '25250101',
				 primary_vendor_ind       => 'Y',
				 local_first_cost         => $cache_upc->{$ctr_upc}->{'anchor_price'}, ### the new cost
				 cost_descriptor          => 'EA',
				 cost_factor              => 1,
				 landed_cost_id           => '0000',
				 exclude_vendor_rebate    => 'N',
				 rtv_allowed_ind          => 'Y',
				 inner_pack_qty           => $cache_upc->{$ctr_upc}->{'inner_pack_qty'},
				 outer_pack_qty           => $cache_upc->{$ctr_upc}->{'outer_pack_qty'},
				 created_by               => 'MERCH',
				 status                   => 'N',
				 process_status           => 'N',
				 vendor_style_no          => $cache_upc->{$ctr_upc}->{'vendor_style_no'}
			     }, 
			     $log,
			    );
		    }
		    
		    if ($cache_upc->{$ctr_upc}->{'record_type'} eq 'DEL'){
			if ( sprintf("%.2f",$cache_upc->{$ctr_upc}->{'anchor_price'}) ne 
			     sprintf("%.2f",$cache_upc->{$ctr_upc}->{'local_first_cost'}) ){
			    
			    &update_status($dbh, $cache_upc->{$ctr_upc}->{'anchor_upc'},'','1','Mclane Cost not the same as RMS cost for Deletion');
			    ### also, need to mark off this upc from the cache_upc
                            $total_upc = &mark_delete_upc_on_total_list($cache_upc->{$ctr_upc}->{'anchor_upc'}, $total_upc);

			    $cache_upc->{$ctr_upc} = undef; ## remove it from the valid list
			    ### next;			  
                    #### this is to check the zone_id following request from Reggie, and Nora on Nov 26, 2021  
			}elsif($zone_id ne &get_zone_id_in_style_vendors( 
				   $cache_upc->{$ctr_upc}->{'style_id'},  
				   $zone_id,
				   $cache_upc->{$ctr_upc}->{'vendor_id'})
			    ){
                  ### here the code for checking zone_id will be added..
                            &update_status($dbh, $cache_upc->{$ctr_upc}->{'anchor_upc'},'','1','ZONE not match for DEL');
                            ### also, need to mark off this upc from the cache_upc

                            $total_upc = &mark_delete_upc_on_total_list($cache_upc->{$ctr_upc}->{'anchor_upc'}, $total_upc);
                            $cache_upc->{$ctr_upc} = undef; ## remove it from the valid list                           			    
			}else{
			&insert_iri_whsle_style_vendors(
			     $dbh, 
		     {
				 business_unit_id         => '30',
				 job_id                   => '999999999999',
				 style_id                 => $cache_upc->{$ctr_upc}->{'style_id'},
				 vendor_id                => $cache_upc->{$ctr_upc}->{'vendor_id'},
				 region_district_sub_type => $region,
				 site_group_id            => $zone_id,
				 start_date               => $cache_upc->{$ctr_upc}->{'start_date'}, 
				 end_date                 => $cache_upc->{$ctr_upc}->{'effective_day'}, #### only this is meaningful
				 primary_vendor_ind       => 'Y',
				 local_first_cost         => $cache_upc->{$ctr_upc}->{'anchor_price'},
				 cost_descriptor          => 'EA',
				 cost_factor              => 1,
				 landed_cost_id           => '0000',
				 exclude_vendor_rebate    => 'N',
				 rtv_allowed_ind          => 'Y',
				 created_by               => 'MERCH',
				 status                   => 'N',
				 process_status           => 'N',
				 vendor_style_no          => $cache_upc->{$ctr_upc}->{'vendor_style_no'}
			     }, 
			     $log,
			    );
			} ## else for cost comparison
		    } ## end of if delete
		} ## end of each zone_id
	    } ## end of if valid group
	} ###foreach cur_group
    }### end of loop upc
    close OUT;  ### close the file for recording all lines into the iri table
    $dbh->disconnect;
    return ($cache_upc, $total_upc);
} ### end of this function...

sub get_zone_id_in_style_vendors{
    my ($style_id, $zone_id, $vendor_id) = @_;
    my $query = "
select site_group_id
from style_vendors
where style_id = \'$style_id\'
and site_group_id =\'$zone_id\'
and vendor_id =\'$vendor_id\'
and rownum < 2";
    my $sth_zone;
    my $data_extraction_error;
    my $zone_site;

    my $dbh = IBIS::DBI->connect( dbname => $g_db_name);
    eval{
        $sth_zone = $dbh->prepare($query);
        $sth_zone->execute;
    };

    if ($@){
        my $msg = "get_zone_id_in_style_vendors error:".$@ ;
        &log_debug ($msg);
        $data_extraction_error = 1;
    }else{
        my $msg = "get_zone_id_in_style extraction success.";
        &log_debug ($msg);
    }

    while (my $r = $sth_zone->fetchrow_hashref) {
        $zone_site = $r->{'site_group_id'};
    }
    $dbh->disconnect;
    return $zone_site;
}


sub get_tomorrow_date_string{    
    my $query ="select to_char(trunc(sysdate + 1), 'YYYYMMDD') as tomorrow_str from dual";
    my $dbh = IBIS::DBI->connect( dbname => $g_db_name);
    my $sth_zone;
    my $data_extraction_error;
    my $date_string;

    eval{
        $sth_zone = $dbh->prepare($query);
        $sth_zone->execute;
    };
    if ($@){
	my $msg = "Date excution error:".$@ ;
	&log_debug ($msg);
	$data_extraction_error = 1;
    }else{
	my $msg = "date extraction success.";
	&log_debug ($msg);
    }

    while (my $r = $sth_zone->fetchrow_hashref) {
	$date_string = $r->{'tomorrow_str'};
    }
    $dbh->disconnect;
    return $date_string;
}


sub mark_delete_upc_on_total_list{
    my ($anchor_upc , $total_upc) = @_;
    print Dumper($total_upc);

    foreach my $ctr_upc(keys %{$total_upc}) {
        if ($total_upc->{$ctr_upc}->{'rejected_ind'}){
            next;
        }else{
	    my $real_upc;

	    if ( $total_upc->{$ctr_upc}->{'upctype'} eq 'SHIP'){ 
		$real_upc  = $total_upc->{$ctr_upc}->{'shipupc'};
	    }
	    if ( $total_upc->{$ctr_upc}->{'upctype'} eq 'PACK'){
		$real_upc  = $total_upc->{$ctr_upc}->{'packupc'};
	    }
	    
	    if ($anchor_upc eq $real_upc){ 
		
		$total_upc->{$ctr_upc}->{'rejected_ind'} = 1;
		$total_upc->{$ctr_upc}->{'reject_reasons'} ='DEL type with different cost from RMS';
	    }
	} ## for else
    }## foreach loop
    return $total_upc;
}




## Create a report required in the SPECs
sub print_report_file{
    my ($cache_upc, $filename,  $s_file) = @_;
    open(OUT, ">$filename") or die "failed to open file to write". $filename;

####my $result_str .= "BUYER|DEPT_ID|DEPT_NAME|RECORD_TYPE|VENDOR_STYLE|DESCRIPTION|UPC_TYPE|MCLANE_UPC|RMS_STYLE|VENDOR|COST|INNER_PACK|OUTER_PACK|EFFECTIVE_DATE|GROUP|STORE|REASON\n";
    my $result_str ='';
    
    ##print OUT $result_str;
    foreach my $ctr_upc( keys %{$cache_upc}) {
        if ($cache_upc->{$ctr_upc}->{'rejected_ind'} == 1){

            my $one_nu_line ='';	
	    my @dept_and_name = split(/\_/, $cache_upc->{$ctr_upc}->{'dcs'});
	    my $buyername ='';
            if ($dept_and_name[0]){
                $buyername = &get_buyername_by_dept_id($dept_and_name[0]);
            }else{
                $buyername ='';
            }

	    print Dumper(\@dept_and_name) if $debug;
	    print "dept0: $dept_and_name[0]\n" if $debug;
	    print "buyername: $buyername\n" if $debug;
            $one_nu_line .= $buyername."|";
            $one_nu_line .= $dept_and_name[0]."|";
	    $one_nu_line .= $dept_and_name[1]."|";
            $one_nu_line .= $cache_upc->{$ctr_upc}->{'record_type'}."|";
            $one_nu_line .= $cache_upc->{$ctr_upc}->{'vendpcode'}."|";
            $one_nu_line .= $cache_upc->{$ctr_upc}->{'vendpdesc'}."|";
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'upctype'}."|";
            $one_nu_line .= $cache_upc->{$ctr_upc}->{'anchorupc'}."|";
            $one_nu_line .= $cache_upc->{$ctr_upc}->{'style_id'}."|";
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'vendor_id'}."|";
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'cost'}."|";
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'inner_pack_qty'}."|";
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'outer_pack_qty'}."|";
	    my $nu_format = &reformat_date_string($cache_upc->{$ctr_upc}->{'effective_day'});
	    $one_nu_line .= $nu_format."|"; #### for rpt2db loading
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'group_id'}."|";
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'store'}."|";
            $one_nu_line .= $cache_upc->{$ctr_upc}->{'reject_reasons'}; 
            $one_nu_line .= "\n";
            print OUT $one_nu_line;
        } ## if xxx == 1
    }## foreach
    close OUT;
    return $filename;
}

sub print_success_report_file{
    my ($cache_upc, $filename, $total_cache_upc, $log) = @_;
    
    my $hash_size = keys %{$cache_upc};
    unless($hash_size){ ### if size is zero, return right away...
	return $filename;
    }
    
    open(OUT, ">$filename") or die "failed to open file to write". $filename;
### collect mclane_information
    my $ref_mclane_info = &get_mclane_info_key_by_anchorupc($log);

    ### print Dumper($ref_mclane_info);
    
### collect rms retail prices
    my $ref_price_info  = &get_price_info_key_by_anchorupc($log);

    ### print Dumper($ref_price_info);
    ## begin to loop

#### my $result_str .= "BUYER|DEPT_ID|DEPT_NAME|VENDOR|RECORD_TYPE|VENDOR_STYLE|DESCRIPTION|UPC_TYPE|MCLANE_UPC|RMS_STYLE|STYLE_TYPE|INNER_PACK|OUTER_PACK|EFFECTIVE_DATE|END_DATE|GROUP|ZONE|STORE|NEW_COST|OLD_COST|COST_VARIANCE|PERCENT_VARIANCE|RETAIL_PRICE|IMU\n";
    my $result_str ='';
    ###print OUT $result_str;
    foreach my $ctr_upc( keys %{$cache_upc}) {
	    ### print Dumper($cache_upc->{$ctr_upc});
	if ($cache_upc->{$ctr_upc}->{'style_id'}){
	    my $one_nu_line ='';

            my @dept_and_name = split(/\_/, $cache_upc->{$ctr_upc}->{'dcs'});
            my $buyername ='';
            if ($dept_and_name[0]){
                $buyername = &get_buyername_by_dept_id($dept_and_name[0]);
            }else{
                $buyername ='';
            }
	    my $vendor_id = $cache_upc->{$ctr_upc}->{'vendor_id'};
	    unless($vendor_id){
		$vendor_id = '00009830555';
	    }
            $one_nu_line .= $buyername."|";
            $one_nu_line .= $dept_and_name[0]."|";
            $one_nu_line .= $dept_and_name[1]."|";

	    $one_nu_line .= $vendor_id ."|";
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'record_type'}."|";
	    my ($v_code, $v_desc);

	    $v_code = $ref_mclane_info->{$cache_upc->{$ctr_upc}->{'anchor_upc'}}->{'vendpcode'};
	    $v_desc = $ref_mclane_info->{$cache_upc->{$ctr_upc}->{'anchor_upc'}}->{'vendpdesc'};
	    $one_nu_line .= $v_code."|";##??
            $one_nu_line .= $v_desc."|";##??
            
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'upc_type'}."|";
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'anchor_upc'}."|";
            $one_nu_line .= $cache_upc->{$ctr_upc}->{'style_id'}."|";
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'style_type'}."|";
            $one_nu_line .= $cache_upc->{$ctr_upc}->{'inner_pack_qty'}."|";
            $one_nu_line .= $cache_upc->{$ctr_upc}->{'outer_pack_qty'}."|";

	    my $nu_format = &reformat_date_string($cache_upc->{$ctr_upc}->{'effective_day'});
            $one_nu_line .= $nu_format."|"; ####for rpt2db loading


	    my $nu_end_date = &reformat_date_string($cache_upc->{$ctr_upc}->{'end_date'});
	    $one_nu_line .= $nu_end_date."|";

            $one_nu_line .= $cache_upc->{$ctr_upc}->{'group_id'}."|";
            my $zone_list = &get_zone_list_by_group_id( $log,
                substr($cache_upc->{$ctr_upc}->{'group_id'}, 
                       length($cache_upc->{$ctr_upc}->{'group_id'}) - 3, 
                       3)
                       );
	    $one_nu_line .= $zone_list."|";### need to derive
            $one_nu_line .= $cache_upc->{$ctr_upc}->{'store'}."|";
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'anchor_price'}."|"; 
	    $one_nu_line .= $cache_upc->{$ctr_upc}->{'local_first_cost'}."|";

            my $variance = $cache_upc->{$ctr_upc}->{'anchor_price'} - 
		$cache_upc->{$ctr_upc}->{'local_first_cost'};
	    $one_nu_line .= sprintf("%.2f",$variance)."|";
	    my $tmp_new_cost = $cache_upc->{$ctr_upc}->{'anchor_price'};
	    unless($cache_upc->{$ctr_upc}->{'anchor_price'}){
		$tmp_new_cost = 1;
	    }
	    my $percent_var = ($variance / $tmp_new_cost) * 100;
	    $one_nu_line .= sprintf("%.2f", $percent_var)."|";

           my $retail_price =
 	    $ref_price_info->{ $cache_upc->{$ctr_upc}->{'anchor_upc'}}->{'retail_price'};
 

	    $one_nu_line .= $retail_price."|";
	    if ($retail_price){
		my $ratio  = ($cache_upc->{$ctr_upc}->{'anchor_price'}/$retail_price);
		my $ratio2 = ($retail_price/$cache_upc->{$ctr_upc}->{'anchor_price'});
		if (($ratio < 2) or ($ratio2 < 2)) {
		    my $tmp_imu = 
                    abs( ($retail_price - $cache_upc->{$ctr_upc}->{'anchor_price'})/$retail_price ) * 100;
		    if ($tmp_imu < 100){
			$one_nu_line .= sprintf("%.2f",$tmp_imu)."|";
		    }else{
			$one_nu_line .= ""."|";
		    }
		}else{
		    $one_nu_line .= ""."|";
		}
	    }else{
		$one_nu_line .= ""."|";
	    }

            $one_nu_line =~ s/\|+$//g; 
            $one_nu_line .= "\n";
            print OUT $one_nu_line;
	}
    }## foreach
    close OUT;
    return $filename;
}

sub reformat_date_string{
    my ($input_date) = @_;
    ### YYYYMMDD ### length 8
    if(length($input_date) == 8){
	my ($mm, $dd, $yyyy);
	$dd = substr($input_date, -2, 2);
	$mm = substr($input_date, -4, 2);
	$yyyy = substr($input_date, 0, 4);
	my $new_str = $mm.'/'.$dd.'/'.$yyyy;
	return $new_str;
    }else{
	return '';
    }
}

sub get_zone_list_by_group_id{
    my ($log, $group_id) = @_;

    my $query = "select zone_site from mclane_group2zonesite where group_id =\'$group_id\' order by zone_site asc";
    my $sth_zone;
    my $data_extraction_error;
    my $zone_list;

    my $dbh = IBIS::DBI->connect( dbname => $g_db_name);
    eval{
	$sth_zone = $dbh->prepare($query);
	$sth_zone->execute;
    };

    if ($@){
	my $msg = "zone data excution error:".$@ ;
	&log_debug ($msg);
	$data_extraction_error = 1;
    }else{
	my $msg = "UPC extraction success.";
	&log_debug ($msg);
    }

    my $ctr_upc = 0; ## set begining counter as 0
    while (my $r = $sth_zone->fetchrow_hashref) {
	$zone_list .= ",".$r->{'zone_site'};
    }
    $zone_list =~ s/^\,//; ## remove the first comma
    $dbh->disconnect;
    return $zone_list;
}


sub get_buyername_by_dept_id{
    my ($dept_id) = @_;
    print "dept_id: $dept_id\n" if $debug;

    my $query = "
      select 
            e.last_name as buyername
      from  departments d, 
            employees e,
            v_dept_class_subclass v
      where d.business_Unit_id = 30 and
            d.business_Unit_id = e.business_Unit_id and
            d.buyer_employee_id = e.employee_id and
            v.DEPARTMENT_ID = d.department_id and 
            d.department_id = \'$dept_id\'
            and rownum < 2";

    ##my $query ="select get_buyername_by_dept_id($dept_id) as buyername from dual";
    my ($sth_info, $buyername);
    my $dbh = IBIS::DBI->connect( dbname => $g_db_name);
    eval{
        $sth_info = $dbh->prepare($query);
        $sth_info->execute;
    };

    if ($@){
        my $msg = "Function call get_buyername_by_dept_id error".$@ ;
        ##&log_debug ($msg);
	print "$msg\n";
    }else{
        my $msg = "Buyername obtained from dept_id";
        ##&log_debug ($msg);
	print "$msg\n" if $debug;
    }

       
    while (my $r = $sth_info->fetchrow_hashref) {
	print "rvalue in function: $r->{'buyername'} next dumper:\n" if $debug;
	print Dumper($r) if $debug;
	$buyername = $r->{'buyername'};
    }
    $dbh->disconnect;
    return $buyername;
}


sub get_mclane_info_key_by_anchorupc{
    my ($log) = @_;
    my $query = "select anchorupc, vendpcode, vendpdesc from v_mclane_pack_and_ship";
    my ($sth_info, $ret);
    my $dbh = IBIS::DBI->connect( dbname => $g_db_name);
    eval{
        $sth_info = $dbh->prepare($query);
        $sth_info->execute;
    };

    if ($@){
        my $msg = "vpcode, vdesc data excution error:".$@ ;
        &log_debug ($msg);
    }else{
        my $msg = "vpcode, vdesc extraction success.";
        &log_debug ($msg);
    }

    my $ctr_upc = 0; ## set begining counter as 0
    while (my $r = $sth_info->fetchrow_hashref) {
	my $anchor_upc = $r->{'anchorupc'};
        $ret->{$anchor_upc}->{'vendpcode'} = $r->{'vendpcode'};
	$ret->{$anchor_upc}->{'vendpdesc'} = $r->{'vendpdesc'};
	$ret->{$anchor_upc}->{'anchorupc'} = $r->{'anchorupc'};
    }
    
    $dbh->disconnect;
    return ($ret);
}



sub get_price_info_key_by_anchorupc{
    my ($log) = @_;
    my $query = "
select 
u2.group_id,
u2.site_group_id,
u2.site_id,
m.vendpcode,
m.anchorupc,
st.style_id,
(select get_dept_and_name_by_style_id(st.style_id) from dual) as dcs,
st.color_id, 
st.size_id,
st.dimension_id,
MERCH.GET_PERMANENT_RETAIL_PRICE('30',u2.site_id, st.style_id, st.color_id, st.size_id, st.DIMENSION_ID, '','') as perm_price,
st.date_created,
substr(m.group_id, length(m.group_id) - 2, 3)
from 
v_mclane_pack_and_ship m,
bar_codes st,
(
select 
u.group_id,
u.site_group_id,
u.site_id
from
(select 
distinct
g.group_id,
s.site_group_id,
min (s.site_id) as site_id
from 
mclane_group2zonesite g,
site_group_details s
where 
g.zone_site = s.SITE_GROUP_ID
and s.site_id in 
(
select s2.site_id
from site_group_details s2
where s2.site_group_id = g.zone_site
) group by g.group_id, s.site_group_id
) U
) U2
where u2.group_id = substr(m.group_id, length(m.group_id) - 2, 3)
    and m.anchorupc = st.bar_code_id
";

my ($sth_info, $ret);
my $dbh = IBIS::DBI->connect( dbname => $g_db_name);
eval{
    $sth_info = $dbh->prepare($query);
    $sth_info->execute;
};

if ($@){
    my $msg = "vpcode, vdesc data excution error:".$@ ;
    &log_debug ($msg);
}else{
    my $msg = "vpcode, vdesc extraction success.";
    &log_debug ($msg);
}

my $ctr_upc = 0; ## set begining counter as 0
while (my $r = $sth_info->fetchrow_hashref) {
    my $anchor_upc = $r->{'anchorupc'};
    $ret->{$anchor_upc}->{'retail_price'}  = $r->{'perm_price'};
    $ret->{$anchor_upc}->{'style_id'}      = $r->{'style_id'};
     $ret->{$anchor_upc}->{'dcs'}          = $r->{'dcs'};
}

$dbh->disconnect;
return ($ret);
}



## loop through cost data of each style_id, from RMS for this vendor, foreach style_id
## do cost comparison, and get inner/outer qty for it from rms. 
 

### loop through each UPC to check cost:
sub check_date_is_current{
    my ($cache_upc, $cache_current) = @_;
    my $dbh = IBIS::DBI->connect( dbname => $g_db_name);
    foreach my $ctr_upc(keys %{$cache_upc}) {
	if ($cache_upc->{$ctr_upc}->{'rejected_ind'}){
	    next;  
	} 
        ### if the date is not valid, or same cost has loaded already...
	my ($not_valid, $invalid_reason) = &single_anchorupc_iscurrent($cache_upc->{$ctr_upc}->{'anchorupc'}, $cache_current);
	if ( $not_valid){
	    print "HHHEEEEEEEEEEEEEEE not current date, or same cost:$cache_upc->{$ctr_upc}->{'style_id'}\n"  if $debug;
	    $cache_upc->{$ctr_upc}->{'reject_reasons'}  .= $invalid_reason;
	    $cache_upc->{$ctr_upc}->{'rejected_ind'}       =  1;
	    &update_status($dbh, $cache_upc->{$ctr_upc}->{'anchorupc'}, '', '1',$invalid_reason);
	}
    }
    $dbh->disconnect;
    return $cache_upc;
}

### loop through all record
sub single_anchorupc_iscurrent{
    my ($anchorupc, $cache_current ) = @_;

    my $not_valid = 1; ## assuming it is not valid in the begining
    my $invalid_reason ='Effective date not current to RMS';
    foreach my $ctr (keys %{$cache_current}){
	if($cache_current->{$ctr}->{'anchorupc'} eq $anchorupc){
	    $not_valid = 0; ## it is valid

       ##     $invalid_reason ='';
       ##	    if ($cache_current->{$ctr}->{'cost'} 
        ## eq $cache_current->{$ctr}->{'local_first_cost'}){
	##	$not_valid = 1; ### this is not valid, since mclane cost is same as that of MCCS 
        ##        $invalid_reason = 'Same Cost already loaded';
	##
        ## }
	
	}
    }
    return ($not_valid,$invalid_reason);
}


sub check_style_setup{
    my ($cache_upc, $cache_style) = @_;
    my $dbh = IBIS::DBI->connect( dbname => $g_db_name);
    foreach my $ctr_upc(keys %{$cache_upc}) {
	if ($cache_upc->{$ctr_upc}->{'rejected_ind'}){
	    next;  
	} 
	if ( &single_style_validation($cache_upc->{$ctr_upc}->{'style_id'}, $cache_style) ){ ## if not valid...
            print "hhhhhaaaaaaaaaaaaaaaa style not set up\n" if $debug;  
	    $cache_upc->{$ctr_upc}->{'reject_reasons'}  .= "Style not setup for Mclane";
	    $cache_upc->{$ctr_upc}->{'rejected_ind'}       = 1;
             &update_status($dbh, $cache_upc->{$ctr_upc}->{'anchorupc'}, '', '1', 'Style not set up');  
	}
    }
    $dbh->disconnect;
    return $cache_upc;
}


######################

## validate upc is in RMS
sub get_upc_in_rms{
my $dbh = IBIS::DBI->connect( dbname => $g_db_name);

my $q_upc             = qq (
select 
/*+ PARALLEL(MCLANE_COST_INPUT, 8) PARALLEL(BAR_CODES, 8) */
distinct
m.shipupc,
m.packupc,
b.style_id,
(select get_dept_and_name_by_style_id(b.style_id) from dual) as dcs,
m.vendpcode,
m.vendpdesc,
m.record_type,
m.retail,
m.cost,
m.deal_price,
m.effective_day,
m.group_id,
m.inner_pack_qty as inner_pack_qty,
m.outer_pack_qty as outer_pack_qty,
m.store as store,
m.upctype as upctype,
s2.style_type
from
v_mclane_pack_and_ship m,
bar_codes b,
styles s2
where
b.bar_code_id = m.anchorupc and
b.style_id = s2.style_id 
);

eval{
    $sth_upc = $dbh->prepare($q_upc);
    $sth_upc->execute;
};

if ($@){
    my $msg = "UPC data excution error:".$@ ;
    &log_debug ($msg);
    $data_extraction_error = 1;
}else{
    my $msg = "UPC extraction success."; 
    &log_debug ($msg);
}

my $cache_upc;
my $ctr_upc = 0; ## set begining counter as 0
while (my $r = $sth_upc->fetchrow_hashref) {
    $cache_upc->{$ctr_upc}->{'shipupc'}            = $r->{'shipupc'};
    $cache_upc->{$ctr_upc}->{'packupc'}            = $r->{'packupc'};
    $cache_upc->{$ctr_upc}->{'style_id'}           = $r->{'style_id'};
    $cache_upc->{$ctr_upc}->{'style_type'}         = $r->{'style_type'};
    $cache_upc->{$ctr_upc}->{'dcs'}                = $r->{'dcs'};
    $cache_upc->{$ctr_upc}->{'store'}              = $r->{'store'};
    $cache_upc->{$ctr_upc}->{'vendpcode'}          = $r->{'vendpcode'};
    $cache_upc->{$ctr_upc}->{'vendpdesc'}          = $r->{'vendpdesc'};
    $cache_upc->{$ctr_upc}->{'record_type'}        = $r->{'record_type'};
    $cache_upc->{$ctr_upc}->{'retail'}             = $r->{'retail'};
    $cache_upc->{$ctr_upc}->{'cost'}               = $r->{'cost'};
    $cache_upc->{$ctr_upc}->{'inner_pack_qty'}     = $r->{'inner_pack_qty'};
    $cache_upc->{$ctr_upc}->{'outer_pack_qty'}     = $r->{'outer_pack_qty'};
    $cache_upc->{$ctr_upc}->{'effective_day'}      = $r->{'effective_day'};
    $cache_upc->{$ctr_upc}->{'group_id'}           = $r->{'group_id'};
    $cache_upc->{$ctr_upc}->{'upctype'}            = $r->{'upctype'};    
    $cache_upc->{$ctr_upc}->{'reject_reasons'}     = '';
    $cache_upc->{$ctr_upc}->{'vendor_id'}          ='00009830555';
    $ctr_upc++;
}

$dbh->disconnect;
return $cache_upc;

}



## validate style is set up for Mclane
sub get_style_set{
# Connect to DB
my $dbh = IBIS::DBI->connect( dbname => $g_db_name );

## if not in the set, then not set up for Mclane
my $q_style_set = qq(
select
distinct
m.packupc,
m.shipupc,
m.upctype,
b.style_id,
s2.style_type,
(select get_dept_and_name_by_style_id(b.style_id) from dual) as dcs,
nvl(s.LOCAL_FIRST_COST,0) as local_first_cost,
to_char(s.start_date,'YYYYMMDD') as start_date,
to_char(s.end_date,  'YYYYMMDD') as end_date
from
bar_codes b,
style_vendors s,
styles s2,
v_mclane_pack_and_ship m
where
  -----using the view to check both shipupc, and packupc
  m.anchorupc = b.BAR_CODE_ID  and
  b.STYLE_ID = s.STYLE_ID   and
  s.style_id = s2.style_id and 
  s.VENDOR_ID ='00009830555' 
    ---and s.start_date < to_date(m.effective_day,'YYYYMMDD')
  ---and s.end_date > to_date(m.effective_day,'YYYYMMDD')
);

eval{
    $sth_style_set = $dbh->prepare($q_style_set);
    $sth_style_set->execute;
};

if ($@){
     my $msg = "STYLE data excution error:".$@ ;
     &log_debug ($msg);
     $data_extraction_error = 1;
}else{
     my $msg = "STYLE extraction success."; 
     &log_debug ($msg);
}

my $cache_style_set;
my $ctr_style_set = 0;

while (my $r = $sth_style_set->fetchrow_hashref) {
    $cache_style_set->{$ctr_style_set}->{'packupc'}                      = $r->{'packupc'};
    $cache_style_set->{$ctr_style_set}->{'shipupc'}                      = $r->{'shipupc'};
    $cache_style_set->{$ctr_style_set}->{'style_id'}                     = $r->{'style_id'};
    $cache_style_set->{$ctr_style_set}->{'style_type'}                   = $r->{'style_type'};
    $cache_style_set->{$ctr_style_set}->{'dcs'}                          = $r->{'dcs'};
    $cache_style_set->{$ctr_style_set}->{'local_first_cost'}             = $r->{'local_first_cost'};
    $cache_style_set->{$ctr_style_set}->{'start_date'}                   = $r->{'start_date'};
    $cache_style_set->{$ctr_style_set}->{'end_date'}                     = $r->{'end_date'};
    $ctr_style_set ++;
}

$dbh->disconnect;
return $cache_style_set;

}


### check effective_day in the new Mclane data is in the future, or current day...
### ASSUMPTION: EFFECTIVE DATE IS TODAY OR IN THE FUTURE???
sub get_date_is_current{

# Connect to DB
my $dbh = IBIS::DBI->connect( dbname => $g_db_name );

## if not in the set, then not set up for Mclane
my $q_data_current = qq(
select 
distinct
m.shipupc,
m.packupc,
m.anchorupc,
m.vendpcode,
m.vendpdesc,
m.record_type,
m.retail,
m.cost,
m.deal_price,
m.effective_day,
m.upctype,
s.local_first_cost
from
bar_codes b,
style_vendors s,
v_mclane_pack_and_ship m
where
  m.anchorupc = b.BAR_CODE_ID  and
  b.STYLE_ID = s.STYLE_ID   and
  s.VENDOR_ID ='00009830555' and
  to_date(m.effective_day,'YYYYMMDD') <= s.end_date  and
  to_date(m.effective_day,'YYYYMMDD') >= s.start_date and
  to_date(m.effective_day,'YYYYMMDD') >= trunc(sysdate) and
  s.end_date >= sysdate
 );
### to_date(m.effective_day,'YYYYMMDD') >= trunc(sysdate + 1)
### assuming the condition on checking the effective date is to
### check if the effective date is current, or in the future...

eval{
    $sth_data_current = $dbh->prepare($q_data_current);
    $sth_data_current->execute;
};

if ($@){
     my $msg = "EffectiveDate excution error:".$@ ;
     &log_debug ($msg);
     $data_extraction_error = 1;
}else{
     my $msg = "EffectiveDate extraction success."; 
    &log_debug ($msg);
}


my $cache_data_current;
my $ctr_data_current = 0;

while (my $r = $sth_data_current->fetchrow_hashref) { 
    ## ONLY TAKES the UPC for considerations:
    $cache_data_current->{$ctr_data_current}->{'anchorupc'}         = $r->{'anchorupc'};
    $cache_data_current->{$ctr_data_current}->{'cost'}              = $r->{'cost'};
    $cache_data_current->{$ctr_data_current}->{'local_first_cost'}  = $r->{'local_first_cost'};
    $ctr_data_current ++;
}

$dbh->disconnect;
return $cache_data_current;
}


## getting inner and outer qty if not in input file, also meet the condition of previous filtering
sub extract_style_cost_from_union{
    my ($log) = @_;
# Connect to DB
    my $dbh = IBIS::DBI->connect( dbname => $g_db_name );
    my $q_style_set = qq (
select
/*+ PARALLEL(V_MCLANE_COST_INPUT_UNION_ALL2, 8) */
distinct
ua.group_id,
ua.shipupc,
ua.packupc,
ua.upc_type,
ua.style_id,
(select get_dept_and_name_by_style_id(ua.style_id) from dual) as dcs,
ua.record_type,
ua.start_date,
ua.end_date,
ua.effective_day,
ua.local_first_cost,
ua.inner_pack_qty,
ua.outer_pack_qty,
ua.cost,
ua.store,
ua.style_type,
ua.anchor_price,
ua.anchor_upc,
ua.vendor_style_no
from
v_mclane_cost_input_union_all2 ua
where
not exists
(
select 1
from mclane_upc_rejected  s
where
s.upc = ua.anchor_upc
)
);

    eval{
	$sth_style_set = $dbh->prepare($q_style_set);
	$sth_style_set->execute;
       };

    if ($@){
        my $msg = "unionall excution error:".$@ ;
        &log_debug ($msg);
	$data_extraction_error = 1;
    }else{
        my $msg = "unionall extraction success."; 
        &log_debug ($msg);
    }

   my $cache_style_set;
   my $ctr_style_set = 0;

  while (my $r = $sth_style_set->fetchrow_hashref) {
      $cache_style_set->{$ctr_style_set}->{'group_id'}                     = $r->{'group_id'};
      $cache_style_set->{$ctr_style_set}->{'shipupc'}                      = $r->{'shipupc'};
      $cache_style_set->{$ctr_style_set}->{'packupc'}                      = $r->{'packupc'};
      $cache_style_set->{$ctr_style_set}->{'upc_type'}                     = $r->{'upc_type'};
      $cache_style_set->{$ctr_style_set}->{'style_id'}                     = $r->{'style_id'};
      $cache_style_set->{$ctr_style_set}->{'record_type'}                  = $r->{'record_type'};
      $cache_style_set->{$ctr_style_set}->{'start_date'}                   = $r->{'start_date'};
      $cache_style_set->{$ctr_style_set}->{'end_date'}                     = $r->{'end_date'};
      $cache_style_set->{$ctr_style_set}->{'effective_day'}                = $r->{'effective_day'};
      $cache_style_set->{$ctr_style_set}->{'local_first_cost'}             = $r->{'local_first_cost'};
      $cache_style_set->{$ctr_style_set}->{'inner_pack_qty'}               = $r->{'inner_pack_qty'};
      $cache_style_set->{$ctr_style_set}->{'outer_pack_qty'}               = $r->{'outer_pack_qty'};
      $cache_style_set->{$ctr_style_set}->{'dcs'}                          = $r->{'dcs'};
      $cache_style_set->{$ctr_style_set}->{'cost'}                         = $r->{'cost'};
      $cache_style_set->{$ctr_style_set}->{'store'}                        = $r->{'store'};     
      $cache_style_set->{$ctr_style_set}->{'style_type'}                   = $r->{'style_type'};
      $cache_style_set->{$ctr_style_set}->{'anchor_price'}                 = $r->{'anchor_price'};
      $cache_style_set->{$ctr_style_set}->{'anchor_upc'}                   = $r->{'anchor_upc'};
      $cache_style_set->{$ctr_style_set}->{'vendor_style_no'}              = $r->{'vendor_style_no'};
      $ctr_style_set ++;
     }
    $dbh->disconnect;
    return ($cache_style_set);
}


sub get_groupid_map{

my $dbh = IBIS::DBI->connect( dbname => $g_db_name);

my $q_group             = qq (
select 
group_id, zone_site 
from mclane_group2zonesite
);

my $sth_group;

eval{
    $sth_group = $dbh->prepare($q_group);
    $sth_group->execute;
};

if ($@){
    my $msg = "zone mapping date error:".$@ ;
    &log_debug ($msg);
    $data_extraction_error = 1;
}else{
    my $msg = " zone mapping extraction success."; 
    &log_debug ($msg);
}

my $cache_group;
my $ret_group;

my $ctr_group = 0; ## set begining counter as 0
while (my $r = $sth_group->fetchrow_hashref) {
    $cache_group->{$ctr_group}->{'group_id'}            = $r->{'group_id'};
    $cache_group->{$ctr_group}->{'zone_site'}            = $r->{'zone_site'};
    $ctr_group++;
     my $z_type ='';
	
     if (substr($r->{'zone_site'},0,2) eq 'PZ'){
       $z_type ='DISTRICT';
     }else{
       $z_type ='SITE';
     }
    unless($ret_group->{ $r->{'group_id'} }){
    $ret_group->{ $r->{'group_id'} }  = [];
    }
    push (@{$ret_group->{$r->{'group_id'}}}, $r->{'zone_site'}."|".$z_type);
}

$dbh->disconnect;
###return $cache_group;
return $ret_group; 
}


#---------------------------------------------------------------------
sub send_mail_with_attachment {
    my $subject = shift;
    my $body = shift;
    my $file = shift;

    my @emails = values( %{$g_report_email} );
    &log_debug ("Sending attachment to:");
    foreach my $e ( sort keys %{$g_report_email} ) {
        &log_debug (" $e ($g_report_email->{$e})");
    }
    &log_debug ("mail_report");

    ###my $subject = "Will be from parameter values";
    
    &log_debug (" Subject: $subject");
    &log_debug (" Attachment: $file") if $file;

    $go_mail->logObj($log);
    $go_mail->subject($subject);
    $go_mail->sendTo($g_report_email);
    $go_mail->attachments($file) if $file;
    $go_mail->msg($body);

    if ($file) {
        $go_mail->send_mail_attachment();
    }
    else {
        $go_mail->send_mail();
    }
}

sub purge_arch_over_60_days{
# Connect to DB
    my $dbh = IBIS::DBI->connect( dbname => $g_db_name, attribs => { AutoCommit => 0 } );
    
## delete data over 60 days old
    my $q_purge = qq (
    delete 
    from 
       mclane_cost_input_arch
    where 
       record_date <= sysdate - 60
    );
    
    eval{
	$sth_style_set = $dbh->prepare($q_purge);
	$sth_style_set->execute;
    };
    if ($@){
	my $msg = "DB error in purging:".$@ ;
	&log_debug ($msg);
    }else{
	my $msg = "Purge data over 60 days successful."; 
	&log_debug ($msg);
        $dbh->commit;
    }
    $dbh->disconnect;   
}


sub archive_and_purge_last_cost{
# Connect to DB
    my $dbh = IBIS::DBI->connect( dbname => $g_db_name, attribs => { AutoCommit => 0 } );
    
## markoff data after processing..
    my $q_update = qq (
    insert into mclane_cost_input_arch
     select * from mclane_cost_input  
     );
    
    eval{
	$sth_style_set = $dbh->prepare($q_update);
	$sth_style_set->execute;
    };
    if ($@){
	my $msg = "DB error in purging:".$@ ;
	&log_debug ($msg);
    }else{
	my $msg = "Purge data over 60 days successful."; 
	&log_debug ($msg);
	$dbh->commit;
    }

    my $q_delete = qq(
     delete from mclane_cost_input
    );

    eval{
        $sth_style_set = $dbh->prepare($q_delete);
        $sth_style_set->execute;
    };
    if ($@){
        my $msg = "DB error in purging:".$@ ;
        &log_debug ($msg);
    }else{
        my $msg = "Purge data over 60 days successful.";
        &log_debug ($msg);
        $dbh->commit;
    }
    $dbh->disconnect;   
}

sub archive_loaded_files{
    my ($day_d)  = @_;
    my $f_files = $g_data_dir."/".$g_file_pattern.'*';
    my $t_files = $g_archive_dir."/".$day_d;
    unless( -e $t_files){
	my $t_dir_cmd = "/bin/mkdir $t_files";
	system($t_dir_cmd);
    }
    my $cmd = 'mv  '.$f_files .'   '. $t_files;
    my $ret = system($cmd);
    unless ($ret){
	my $msg = "system command call success. ".$cmd;
	&log_debug ($msg);
    }else{
	my $msg = "system command call failed. ".$cmd;
	&log_debug ($msg);
	&send_mail_with_attachment ('Failed to move away loaded mclane cost file', 'Failure in running '.$cmd, '');
    }
}


# Add a line status at the end of each of the record                                                                                                         
sub add_status_for_cost_file{
    my ($infile) = @_;

    open(IN, "<$infile" ) or die "failed to open infile.". $infile;
    my @ary = <IN>;
    close IN;

    my $outfile = $infile.".mod";
    open(OUT, " > $outfile" ) or die "failed to open file to write.". $outfile;
    foreach my $line(@ary){  
	if( $line =~ /\d+/g){
	    $line =~ s/\"//g;
            $line =~ s/\r\n?//g;
            ###$line =~ s/\n//g;
            $line = $line.",N\n";
            print OUT $line;
	}
    }
    close OUT;
## move away the orginal file to archive directory: 
    if ( -s $outfile ){
	my $mv_cmd = " mv $infile  $g_archive_dir";
	system($mv_cmd);
    }

}

##################end of subs ###############################################
### Query after insertion into IRI_WHSLE_STYLE_VENDORS:
### select * from IRI_WHSLE_STYLE_VENDORS where date_created > sysdate - 1/12;
##############################################################################


__END__

=pod

=head1 NAME

mclane_cost_load.pl

-- Upload cost file from Mclane into iri table for cost update in RMS.

=head1 VERSION

This program is the initial version 1.0. 

=head1 USAGE

perl /usr/local/mccs/bin/mclane_cost_load.pl
  
=head1 REQUIRED ARGUMENTS

None.

=head1 DESCRIPTION

=over

=item Based on Project specification Mclane Cost Load Requirements (v2.1 Dec 2019),  by Alicia Morrison.

=item Basically, the program is doing a sequential processing and there are dependence between steps. Even not all the steps are necessary, these steps are what the specs requires. 

=item Here are the processing steps in the program:

=item 1, Verify if each shipUPC and packUPC from Mclane exists in bar_codes RMS table.
 
=item  $cache_upc = &get_upc_in_rms();

=item  Mark off the ones do not have matching UPCs in RMS as a condition regardless if the vendor is from Mclane. If style_id is found in RMS, it counts as found.

=item $cache_upc = &check_upc($cache_upc);

=item  2, Verify styles associated to the UPC is setup for Mclane in RMS.

=item  a, get all  styles for mclane in style_vendors table, matching style for the upc in bar_codes and matching upc in mclane data. This has to be Mclane vendor id in style vendors table.

=item $cache_style = &get_style_set();

=item b, loop through all the mclane styles from the file, mark off any styles not in the list from step a.

=item $cache_upc = &check_style_setup($cache_upc, $cache_style);

=item a, get all styles with effective date is current, based on conditions of step 2, i.e., three tabl\
es, mclane_cost_input, bar_codes, and vendor_styles. Also, the effective date from Mclane is between th\
e starting date and ending date in vendor_styles table.

=item $cache_iscurrent = &get_date_is_current();

=item b, mark off any styles which is not in the above list, as invalid.

=item $cache_upc = &check_date_is_current($cache_upc,$cache_iscurrent);



=item extract_data_from_a_union

=item 
 extracting data from a union of all cases of upcs needed cost update including: oneliner per packupc (pack and ship both),  multiliner per packupc (for multi, use shipupc only, as single packupc are as shipupc repeated in mclane file)
=item   the view decided anchor upc, anchor price, style_id for loading
=item   here is the logic to decide anchor_upc, and anchor_price,
=item   style_id matches only to anchor_upc
=item   if upc_type is ship, then anchor upc is shipupc
=item   if upc_type is pack, then anchor upc is packupc
=item   if style_type is single, or regular, then, unit cost is calculated cost/innerXouter.
=item   if style_type is multi, then, cost  is used directly for the whole shipupc.


=item  5, Load the data items not being rejected after the 4 steps into table: iri_whsle_style_vendors. For deletion cost type, use the effective date as the end_date of that style.
 
=item  &collect_data_for_iri_whsle($cache_upc, $log);

=item send out email if any errors happen in db operations in the above process.

=item 

=item 6, Calculate the ratios of cost changes for any rejected items.
 then, create an report file.  send out the report as an email attachment. then, archive the file into archive directory.

=item &print_report_file($cache_upc, $report_file);

=item

=item 7, markoff the data rows processed today in Mclane_cost_input table, status from 'N' to 'P'(a daily process).

=item  &markoff_processed_data();

=item After that, archive away files, including: cost_file, mod_cost_file, report_file and debug_file, in the staging directory. only if found new files, all other files should be produced.

=item    &archive_loaded_files();

=item 

=item The above is the whole process in very detailed description.

=back

=head1 REQUIREMENTS

None.

=head1 DIAGNOSTICS

None.

=head1 CONFIGURATION

=over 4

=item  Here is the list of details for production:

=item  An cost table, Mclane_cost_input, and an archive table, Mclane_cost_input_archive need to be created in production database.

=item  Entries in ibisxml.cfg needed for remote server, and directory path related to the process.

=item  A config file contains log, database connection_id, and database table name, data delimiter for the process and related data loading.                                       
 
=back
                                                                                        
=head1 DEPENDENCIES

=over 4

=item * POSIX qw(strftime WNOHANG) for getting time string

=item * IBIS::Log::File for using log
 
=item * MCCS::DBI  for connecting to database server

=item * Net::SFTP::Foreign for fetching cost file from Mclane

=back

=head1 SEE ALSO 

=over

=item Some emails and meeting notes maybe added later. 

=back

=head1 INCOMPATIBILITIES

Unknown.

=head1 BUGS AND LIMITATIONS

Limitations:

1, Still waiting for final test data file from Mclane. Also, RMS database preparation is still needed as informed in the meeting with M.T. 
2, Report problems to <MCCS rdistaff@usmc-mccs.org> if running into errors.

=head1 BUSINESS PROCESS OWNER

 Chunhui Yu<yuc@usmc-mccs.org>
 Alicia Morrison<alicia.morison@usmc-mccs.org>

=head1 AUTHOR

Chunhui Yu<yuc@usmc-mccs.org>

=head1 ACKNOWLEDGEMENTS

=over 

=item Collegues: Armando Someillan, Mike Thomas, Alice Morrison etc. 

=back

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2020 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT 
SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION 
OF CONTRACT, OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
DEALINGS IN THE SOFTWARE.

=cut

