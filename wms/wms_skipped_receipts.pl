#!/usr/local/mccs/perl/bin/perl
##--------------------------------------------------------------------------
##Ported by : Soumya K Bhowmic
##Date      : 12/11/2023
##
##Brief Desc: Generate/send email with html report showing receipts that are 
##            delayed in posting to RMS for  what is considered an abnormal 
##            length of time, as set in ibiscfg file
##
##            The program must run a rdiusr.
## --------------------------------------------------------------------------  
#---------------------------------------------------------------------
# Program:  wms_skipped_receipts.pl
# Author:   Armando Someillan
# Created:  Mon June 26th, 2017
# Description: Generate/send email with html report showing receipts that are delayed in posting to RMS for 
# what is considered an abnormal length of time, as set in ibiscfg file
#---------------------------------------------------------------------
use strict;
use IBIS::DBI;
use IBIS::Log::File;
use File::Basename;
use File::Path;
use Readonly;
use MCCS::Config;
use Data::Dumper;
use Fcntl qw(:flock);
use MCCS::WMS::Sendmail;
use Getopt::Long;

# Flush output
$| = 1;

#- One process at a time ---------------------------------------------
my $lock_file = "/usr/local/mccs/data/wms/tmp/" . basename($0) . ".lck";
open SELF, "> $lock_file" or die "Could not create lock file $lock_file";
flock SELF, LOCK_EX | LOCK_NB or die "Another $0 process already running";

#handle command line arguments
#The job id as part of getoptions (next twolines) is nnever used, and therefore has been commented out.
#my $job_id;
#my $options = ( GetOptions( 'job_id=s' => \$job_id) );

#- Configuration files -----------------------------------------------
my $g_cfg     = new MCCS::Config;
#TODO Made changes to following three lines, 
#uncomment next line, and delete the two lines afer that.
#my $g_emails  = $g_cfg->WMS_SKP_RECEIPTS->{emails};
my $g_emails;  
$g_emails->{kav} ='kaveh.sari@usmc-mccs.org';

my $g_dbname = $g_cfg->WMS_SKP_RECEIPTS->{db_name};
my $g_age_mins = $g_cfg->WMS_SKP_RECEIPTS->{arc_age_mins};

#print Dumper $g_emails;

#- Global variables --------------------------------------------------
#g_verbose is never changed in this program, not sure why it's here.
my $g_verbose = 0;
Readonly my $g_logfile => '/usr/local/mccs/log/wms/' . basename(__FILE__) . '.log';
my $g_long_date = `date +"%D %r"`;
chomp($g_long_date);
my $g_log = IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );
my $g_host = `hostname`;
chomp($g_host);
my $go_mail = MCCS::WMS::Sendmail->new();

#---------------------------------------------------------------------
# SUBS
#---------------------------------------------------------------------
sub send_mail {
    my $msg_sub  = shift;
    my $msg_bod1 = shift;
    my $msg_bod2 = shift || '';
    my @body     = ( $msg_bod1, $msg_bod2 );

    return if $g_verbose;    # Dont want to send email if on verbose mode

    $go_mail->logObj($g_log);
    $go_mail->subject($msg_sub);
    $go_mail->sendTo($g_emails);
    $go_mail->msg(@body);
    $go_mail->hostName($g_host);
    $go_mail->send_mail();
}

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

sub get_error_msg {
    my $sth   = shift;
    my $rejected_id = shift;
    my $msg     = '';
    

    $sth->execute($rejected_id);

    while ( my $a = $sth->fetchrow ) {
        $msg .= $a . "<br>";
    }

    return $msg;

}

#---------------------------------------------------------------------
sub fatal_error {
    my $msg = shift;
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $msg );
    $g_log->error($msg);
    die $msg;
}

#---------------------------------------------------------------------
#---------------------------------------------------------------------

#---------------------------------------------------------------------
# Pseudo Main which is called from MAIN
#---------------------------------------------------------------------
sub my_main {
    #The next line is of no vlaue because the variable $sub is never used.
    #my $sub = __FILE__ . " the_main";
    if ($g_verbose) {

        # Record Everything
        $g_log->level(5);
    }
    else {

        # Record Everything, except debug logs
        $g_log->level(4);
    }

    $g_log->info("-- Start ----------------------------------------");
    $g_log->info("Database = $g_dbname");
    my $dbh = IBIS::DBI->connect(dbname=> $g_dbname);
    my $sql = "select distinct rarc.receipt_id, qarc.host_group_id,round((sysdate-qarc.date_finished) * 1440) age_in_minutes 
    from t_al_host_sql_export_queue_arc qarc
join t_al_host_receipt_arc rarc on rarc.host_group_id = qarc.host_group_id
left join receipts r on r.business_unit_id = '30' and r.receipt_id = rarc.receipt_id
left join merch.iri_wms_receipts iriw on iriw.business_unit_id = '30' and iriw.receipt_id = rarc.receipt_id
where qarc.export_type = 'RECEIPT' and ((sysdate-qarc.date_finished) * 1440) >= ? 
and  to_char(Qarc.Date_Inserted,'yyyy') >=  to_char(add_months(trunc(sysdate), -12*2), 'yyyy')
and r.receipt_id is null and iriw.receipt_id is null and is_number(rarc.po_number) = 'Y'";
  
    
    my $sth = $dbh->prepare($sql);
    
    $sth->execute($g_age_mins);
    my @email_body = ();

    my $from = 'rdistaff@usmc-mccs.org';

    my $css = <<ECSS;
<style>
p, body {
    color: #000000;
    font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
}

.e832_table_nh {
    font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
    font-size: 15px;
    border-collapse: collapse;
    border: 1px solid #69c;
    margin-right: auto;
    margin-left: auto;
}
.e832_table_nh caption {
    font-size: 15pt;
    font-weight: bold;
    padding: 12px 17px 5px 17px;
    color: #039;
}
.e832_table_nh th {
    padding: 1px 4px 0px 4px;
    background-color: RoyalBlue;
    font-weight: normal;
    font-size: 15px;
    color: #FFF;
}
.e832_table_nh tr:hover td {
    /*
    color: #339;
    background: #d0dafd;
    padding: 2px 4px 2px 4px;
    */
}
.e832_table_nh td {
    padding: 2px 3px 1px 3px;
    color: #000;
    background: #fff;
}
</style>
ECSS
    my $tmp = '';
    while ( my $row = $sth->fetchrow_hashref ) {
        $tmp .= '<tr>';
        $tmp .= '<td align=right>' . $row->{receipt_id} . '</td>';
        $tmp .= '<td align=right>' . $row->{host_group_id} . '</td>';
        $tmp .= '<td align=right>' . $row->{age_in_minutes} . '</td>';
        $tmp .= '</tr>' . "\n";
    }
    my $timestamp = `date`;
    my $msg       = <<EOM;
<p style="font-size: 11px;">WMS Receipts delayed in posting for an unusual length of time</p>
<table class="e832_table_nh">
        <tr>
            <th>receipt_id</th>
            <th>host_group_id</th>
            <th>age_in_minutes</th>
        </tr>
        $tmp
</table>
<p style="font-size: 11px;">$timestamp</p>
<p style="font-size: 10px;">server: $g_host</p>
EOM

    print $msg if $g_verbose;

    unless ($g_verbose) {

        # Only send email if there are records there!

        if ($tmp) {
            my $subject = "Unposted WMS Receipts that were fetched at least " . $g_age_mins . " minutes ago!";
            
            foreach my $name ( sort values %{$g_emails} ) {
            open( MAIL, "|/usr/sbin/sendmail -t" );
            
            ## Mail Header
            print MAIL "To: " . $name . "\n";
            print MAIL "From: $from\n";
            print MAIL "Subject: $subject\n";

            ## Mail Body
            print MAIL "Content-Type: text/html; charset=ISO-8859-1\n\n"
              . "<html><head>$css</head><body>$msg</body></html>";
            close(MAIL);
            
            }

          }

    }
    $g_log->info("-- End ------------------------------------------");

}

#---------------------------------------------------------------------
# MAIN PROGRAM - Do not modify main below.
# Use the main above aka my_main
#---------------------------------------------------------------------

# Want to catch warning
$SIG{__WARN__} = sub { $g_log->warn("@_") };

# Execute the main
eval { my_main() };
if ($@) {
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, "Untrapped Error:\n\n", " $@" );
    $g_log->info($@);
}

#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------
