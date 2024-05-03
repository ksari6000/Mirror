#!/usr/local/mccs/perl/bin/perl
##--------------------------------------------------------------------------
##Ported by : Soumya K Bhowmic
##Date      : 12/11/2023
##
##Brief Desc: This program extracts data for Inventory Adjustments for
##            East Coase RDC (Site ID 60001) and West Coast RDC (Site ID 70001)
#             for SYSTEM Date - 1 where 
#             a. inventory Adjustment quantity >= 50
#             OR
#             b. cost adjustment is >= 500
##            Then generates into a report and sends it Logistic Inventory
##            Controller.
##
##            The program must run a rdiusr.
## --------------------------------------------------------------------------  
##---------------------------------------------------------------------------
## Program    : inv_adj_alert.pl
## Author     : Hanny Januarius
## Created    : Thu Jan 18 13:34:43 EST 2018
##
## Description: Send email to Doug Good at 5 am
##              
##             
##            
##
# Requestor  : Armando Someillan (POC)
#
##---------------------------------------------------------------------------

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
use Getopt::Std;
use DateTime;

# Flush output
$| = 1;

#- One process at a time ---------------------------------------------
my $lock_file = "/usr/local/mccs/tmp/" . basename($0) . ".lck";
open SELF, "> $lock_file" or die "Could not create lock file $lock_file";
flock SELF, LOCK_EX | LOCK_NB or die "Another $0 process already running";

#- Get option switches -----------------------------------------------
our %g_opt = (
    d => 0
);
getopts('d', \%g_opt);
my $DEBUG = $g_opt{d};

#- Configuration files -----------------------------------------------
my $g_cfg = new MCCS::Config;

my $g_emails      = $g_cfg->inv_adj_alert->{tech_emails};  #TODO
my $g_cust_emails = $g_cfg->inv_adj_alert->{customer_emails};  #TODO
my $g_dbname      = $g_cfg->inv_adj_alert->{dbname};  #TODO

if ($DEBUG) {
    print Dumper $g_cfg->inv_adj_alert;
}

#- Global variables --------------------------------------------------
my $g_verbose = 0;
if ( $DEBUG ) {
   $g_verbose = 1;
}
my $progname = basename(__FILE__);
$progname =~ s/\.\w+$//;
Readonly my $g_logfile => '/usr/local/mccs/log/wms/' . $progname . '.log';
my $g_long_date = `date +"%D %r"`;
chomp($g_long_date);
my $g_log = IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );
my $g_host = `hostname`; 
chomp($g_host);
my $go_mail = MCCS::WMS::Sendmail->new();
my $g_dbh = IBIS::DBI->connect(dbname=>$g_dbname);
my $g_sth;

#---------------------------------------------------------------------
# SUBS
#---------------------------------------------------------------------
sub send_mail_html {
    my $msg_sub  = shift;
    my $msg_bod1 = shift;
    my $msg_bod2 = shift || '';

    return if $g_verbose;    # Dont want to send email if on verbose mode

    my $css     = <<ECSS;
<style>
p, body {
    color: #000000;
    font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
}

.e832_table_nh {
    font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
    font-size: 11px;
    border-collapse: collapse;
    border: 1px solid #69c;
    margin-right: auto;
    margin-left: auto;
}
.e832_table_nh caption {
    background-color: #FFF;
    font-size: 11pt;
    font-weight: bold;
    padding: 12px 17px 5px 17px;
    color: #039;
}
.e832_table_nh th {
    padding: 1px 4px 0px 4px;
    background-color: RoyalBlue;
    font-weight: normal;
    font-size: 11px;
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
    padding: 2px 4px 1px 4px;
    color: #000;
    background: #fff;
}
</style>
ECSS
      foreach my $k (keys %{$g_cust_emails}) {
        $g_log->info("Sending to $k email $g_cust_emails->{$k}");
        
        open( MAIL, "|/usr/sbin/sendmail -t" );
        print MAIL "To: " . $g_cust_emails->{$k} . " \n";
        print MAIL "From: rdistaff\@usmc-mccs.org\n";
        #print MAIL "Cc: " . $g_emails->{'Hanny Januarius'} . " \n";
        print MAIL "Subject: $msg_sub \n";
        print MAIL "Content-Type: text/html; charset=ISO-8859-1\n\n"
          . "<html><head>$css</head><body>$msg_bod1 $msg_bod2</body></html>";
        print MAIL "\n";
        print MAIL "\n";
        print MAIL "Server: $g_host\n";
        print MAIL "\n";
        print MAIL "\n";
        close(MAIL);
      }
    $g_log->info("$msg_bod1");

}
#---------------------------------------------------------------------
sub send_mail {
    my $msg_sub  = shift;
    my $msg_bod1 = shift;
    my $msg_bod2 = shift || '';
    my @body = ($msg_bod1, $msg_bod2);

    return if $g_verbose;    # Dont want to send email if on verbose mode

    $go_mail->logObj($g_log);
    $go_mail->subject($msg_sub);
    $go_mail->sendTo($g_emails);
    $go_mail->msg(@body);
    $go_mail->hostName($g_host);
    $go_mail->send_mail();
}

#---------------------------------------------------------------------
sub fatal_error {
    my $msg = shift;
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $msg );
    $g_log->error($msg);
    die $msg;
}

#---------------------------------------------------------------------
sub prepare_sql {

  my $sql = <<ENDSQL;
select STYLE_ID,
       color_id,
       SIZE_ID, 
       DIMENSION_ID,
       DESCRIPTION,
       DEPTID,
       site_id,
       SUM(ADJ_QTY) TOT_ADJ_QTY,
       SUM(extended_absolute_cost) TOTAL_ABSOLUTE_ADJUSTED_CODE 
from
        (select iad.STYLE_ID,
                iad.color_id,
                iad.SIZE_ID, 
                iad.DIMENSION_ID,
                st.DESCRIPTION,
                h.DEPTID,
                iad.site_id,
                iad.ITEM_QTY ADJ_QTY,
                st.ESTIMATED_LANDED_COST*iad.ITEM_QTY extended_absolute_cost 
         from INVENTORY_ADJUSTMENTS iah
              join INVENTORY_ADJUST_DETAILS iad 
               on iad.BUSINESS_UNIT_ID = '30' 
               and iad.INVENTORY_ADJUST_ID = iah.INVENTORY_ADJUST_ID
               and iad.SITE_ID in ('60001', '70001')
              join styles st 
               on st.BUSINESS_UNIT_ID = '30' 
               and st.STYLE_ID = iad.STYLE_ID
              join RDIUSR.TE_ITEM_HIER_ALL h 
               on h.STYLEID = iad.STYLE_ID
where  iah.BUSINESS_UNIT_ID = '30' 
  and trunc(iah.inventory_adjust_date) = trunc( sysdate - 1 ))
group by STYLE_ID, color_id, SIZE_ID, DIMENSION_ID, DESCRIPTION, DEPTID, site_id
having abs(SUM(ADJ_QTY)) >= 50 or abs(SUM(extended_absolute_cost)) >= 500

ENDSQL

    $g_sth = $g_dbh->prepare($sql);
    $g_log->info($sql) ;

}
#---------------------------------------------------------------------
sub get_rec {

    $g_sth->execute();

    my @sorted = ();

    while( my $e = $g_sth->fetchrow_hashref()) {
        push(@sorted, $e);
    }

    my $n = 0;
    my $html;
    foreach my $i (@sorted) {
        $n++;
        $html .= "<tr>\n";
        $html .= qq(    <td align="right" style="color: orange;">$n</td>\n);
        $html .=  "    <td>$i->{style_id}</td>\n";
        $html .=  "    <td>$i->{color_id}</td>\n";
        $html .=  "    <td>$i->{size_id}</td>\n";
        $html .=  "    <td>$i->{dimension_id}</td>\n";
        $html .=  "    <td>$i->{description}</td>\n";
        $html .=  "    <td>$i->{deptid}</td>\n";
        $html .=  "    <td>$i->{site_id}</td>\n";
        $html .=  "    <td align='right'>$i->{tot_adj_qty}</td>\n"
                 ."    <td align='right'>$i->{total_absolute_adjusted_code}</td>\n"
                 ."</tr>\n";
    }

    if (@sorted) {
       # It means we have missing record
       # so let's send email
       
       $html = qq(<tr>\n) 
               . qq(<th>&nbsp;</th>\n) 
               . qq(<th>Style</th>\n) 
               . qq(<th>Color</th>\n) 
               . qq(<th>Size</th>\n) 
               . qq(<th>Dim</th>\n) 
               . qq(<th>Description</th>\n) 
               . qq(<th>Dept</th>\n) 
               . qq(<th>Site</th>\n) 
               . qq(<th title="Total Adjustment Quantity">TAQ</th>\n) 
               . qq(<th title="Total absolute adjustment code">TAAC</th>\n) 
               . qq(</tr>\n) 
               . $html;

       $html = qq(<table class="e832_table_nh" >\n) . $html . "</table><br><p>program: $0</p><br>\n";

       my $subject = qq(Inventory Adjustment Alert);
       my $msg = <<'ENDMSG';

<p>RDC Inventory Adjustments exceeding 50 units or $500</p>

ENDMSG

       $g_log->info($html);
       send_mail_html($subject, $msg, $html);
    }

}
#---------------------------------------------------------------------

#---------------------------------------------------------------------

#---------------------------------------------------------------------
# Pseudo Main which is called from MAIN
#---------------------------------------------------------------------
sub my_main {
    my $sub = __FILE__ . " the_main";
    if ($g_verbose) {

        # Record Everything
        $g_log->level(5);
    }
    else {

        # Record Everything, except debug logs
        $g_log->level(4);
    }

    $g_log->info("-- Start ----------------------------------------");
    prepare_sql();
    get_rec();
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
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date,
        "Untrapped Error:\n\n", " $@" );
    $g_log->info($@);
}

#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------
