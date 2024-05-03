#!/usr/local/mccs/perl/bin/perl
##--------------------------------------------------------------------------
##Ported by : Soumya K Bhowmic
##Date      : 12/11/2023
##
## Brief Desc  Send email to MSST and others notifiying wms items that
#              are missed due to records were created after item send
#              cron job at 7pm.  Any record created after 7 pm should
#              be listed in this report.
#
##            The program must run a rdiusr.
## --------------------------------------------------------------------------  
## --------------------------------------------------------------------------
# Program    : missing_item_report.pl
# Author     : Hanny Januarius
# Created    : Fri Dec 29 07:26:39 EST 2017
#
# Description: Send email to MSST and others notifiying wms items that
#              are missed due to records were created after item send
#              cron job at 7pm.  Any record created after 7 pm should
#              be listed in this report.
#
# Requestor  : Alicia Morrison (POC)
#
## --------------------------------------------------------------------------
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

my $g_emails      = $g_cfg->wms_missing_item->{tech_emails};  #TODO
my $g_cust_emails = $g_cfg->wms_missing_item->{customer_emails};  #TODO
my $g_dbname      = $g_cfg->wms_missing_item->{dbname};  #TODO
   $g_dbname = 'rms_p';
    
if ($DEBUG) {
    print Dumper $g_cfg->wms_missing_item;
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
my $g_yyyymmdd  = `date +"%Y%m%d"`;
chomp($g_yyyymmdd);
my $g_log = IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );
my $g_host = `hostname`; 
chomp($g_host);
my $go_mail = MCCS::WMS::Sendmail->new();
my $g_dbh = IBIS::DBI->connect(dbname=>$g_dbname);
my $g_ba_sth;
my $g_ba_arc_sth;

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
    padding: 2px 3px 1px 3px;
    color: #000;
    background: #fff;
}
</style>
ECSS
    foreach my $name (sort keys %{$g_cust_emails}) {

        $g_log->info("Send email to $name " . $g_cust_emails->{$name} );

        open( MAIL, "|/usr/sbin/sendmail -t" );
        print MAIL "To: " . $g_cust_emails->{$name} . " \n";
        print MAIL "From: rdistaff\@usmc-mccs.org\n";
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

    #return if $g_verbose;    # Dont want to send email if on verbose mode

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

  my $ba_sql = <<ENDSQLBA;
select distinct
           s.hazardous_id,          s.description,         s.weight,
           s.cube,                  tih.lobid,             tih.lobdesc,
           s.estimated_landed_cost, b.style_id,            b.color_id,
           s.width,                 s.height,              b.bar_code_id,
           b.size_id,               nvl(b.dimension_id,'') dimension_id,
           decode(bca.operation_type, 'ADD', 'N',
                                      'UPDATE', 'U',
                                      'DELETE', 'D', ''
                 ) activity_type, trunc(operation_date) operation_date
    from
           bar_codes          b,
           bar_codes_audit    bca,
           styles             s,
           te_item_hier_all       tih
    where
           b.bar_code_id        = bca.bar_code_id
--and s.style_id in ( '08097041354279', '08097041372619', '08097041320069', '08097041626839', '08097041968939')
       and not regexp_like(b.bar_code_id,'[^0-9]+')
       and b.sub_type           in ('UPCA','EAN')
       and
        (
          ( (b.sub_type = 'UPCA') AND  ( length(b.bar_code_id) = 12) )
           OR
          ( (b.sub_type = 'EAN') AND  ( length(b.bar_code_id) = 13) )
        )
       and bca.operation_type           = 'ADD'
       and bca.business_unit_id         = '30'

       and bca.operation_date    BETWEEN TO_DATE('$g_yyyymmdd 19:00:00', 'YYYYmmdd HH24:MI:SS') 
                                     AND TO_DATE('$g_yyyymmdd 23:59:59', 'YYYYmmdd HH24:MI:SS')

       and bca.style_id                 = s.style_id
       and s.business_unit_id           = '30'

       and not exists (
                select  'x'
                from    breakup_details
                where   business_unit_id = 30
                and     breakup_style_id = s.style_id
       )
       and tih.styleid                  = s.style_id

       ---------------------------------------------------------------
       -- Do not want to include the same barcode, same color size dim
       -- ADD and DELETE type
       ---------------------------------------------------------------
       and not  exists (
            select 'y'
            from
                    bar_codes_audit zz
            where   zz.bar_code_id = bca.bar_code_id
            and     zz.color_id    = bca.color_id
            and     zz.size_id     = bca.size_id
            and     zz.style_id    = bca.style_id
            and     zz.operation_type in ('ADD','DELETE')
            and     nvl(zz.dimension_id, 'zxcv') = nvl(bca.dimension_id, 'zxcv')

       and zz.operation_date    BETWEEN TO_DATE('$g_yyyymmdd 19:00:00', 'YYYYmmdd HH24:MI:SS') 
                                     AND TO_DATE('$g_yyyymmdd 23:59:59', 'YYYYmmdd HH24:MI:SS')

            group by
                    zz.style_id, zz.bar_code_id, zz.color_id,
                    zz.size_id,  nvl(zz.dimension_id, 'zxcv')
            having mod(count(*),2) = 0
       )
       and 
       
       ---------------------------------------------------------
       -- Don't want to select records which already selected by
       -- description or hazard id changes
       ---------------------------------------------------------
       not exists (
           select 'x'
           from
               bar_codes          bb,
               styles             ss,
               te_item_hier_all       tih
           where
                 not regexp_like(bb.bar_code_id,'[^0-9]+')
           and ss.style_id               = s.style_id
           and bb.bar_code_id            = b.bar_code_id
           and nvl(bb.color_id,'aa')     = nvl(b.color_id,'aa')
           and nvl(bb.size_id,'aa')      = nvl(b.size_id,'aa')
           and nvl(bb.dimension_id,'aa') = nvl(b.dimension_id,'aa')
           and
                 ss.style_id       in
                 (
                  -- Find all styles that have its description changed
                  --
                  select substr(ta.primary_key,4)
                  from   table_audits ta
                  where  ta.business_unit_id = '30'
                  and    ta.application_id   = 'RAMS'
                  and    ta.table_name       = 'STYLES'
                  and (  ta.column_name      = 'DESCRIPTION'
                         or
                         ta.column_name      = 'HAZARDOUS_ID'
                       )
                  and    ta.operation        = 'UPDATE'

       and ta.operation_date    BETWEEN TO_DATE('$g_yyyymmdd 19:00:00', 'YYYYmmdd HH24:MI:SS') 
                                     AND TO_DATE('$g_yyyymmdd 23:59:59', 'YYYYmmdd HH24:MI:SS')

                 )
           and bb.style_id           = ss.style_id
           and bb.sub_type           in ('UPCA','EAN')
           and ss.business_unit_id   = '30'
           and bb.business_unit_id   = '30'
           and tih.styleid           = ss.style_id
           and not exists (
                    select  'x'
                    from    breakup_details
                    where   business_unit_id = 30
                    and     breakup_style_id = ss.style_id
           )
      )
ENDSQLBA

    $g_ba_sth = $g_dbh->prepare($ba_sql);
    $g_log->info($ba_sql) if $DEBUG;

    my $ba_arc_sql = <<ENDSQLBAARC;
select  *
from    t_al_host_item_master_arc
where   
        host_group_id like 'mcx_item_dl_ba_exp%'
    and trunc(RECORD_CREATE_DATE)    = trunc(sysdate)
    and wh_id = '60001xxxxx'  -- just to get zero recs

    -- and trunc(RECORD_CREATE_DATE)    = trunc(to_date('08/15/2020','MM/DD/YYYY'))

ENDSQLBAARC

    $g_ba_arc_sth = $g_dbh->prepare($ba_arc_sql);
    $g_log->info("--------------------------------------------------------");
    $g_log->info("\n" . $ba_arc_sql);

}
#---------------------------------------------------------------------
sub get_ba_rec {

    $g_log->info("Getting BA record from $g_dbname");
    $g_ba_sth->execute();
    my @a = ();

    while( my $r = $g_ba_sth->fetchrow_hashref) {
        my $concat = $r->{style_id} . $r->{color_id} .$r->{size_id} .$r->{dimension_id};
        $concat =~ s/\s+$//g;
        $concat =~ s/^\s+//g;
        $r->{item_number} = $concat;
        push( @a, $r);
        #print "$r->{style_id}  item_id = $concat   barcode = $r->{bar_code_id}\n"; 
    }

    $g_log->info("Getting BA record from WMS-Archive");
    $g_ba_arc_sth->execute();

    
    my @b = ();
    my %arc_item_number = ();
    while( my $r = $g_ba_arc_sth->fetchrow_hashref) {
        push( @b, $r);
        $arc_item_number{$r->{item_number}}++;
    }

    # print Dumper \%arc_item_number;

    # Let's see if all we have in the Archive is the one that we sent
    #----------------------------------------------------------------
    my @z = ();
    foreach my $e( @a) {
        if ( exists($arc_item_number{$e->{item_number}} ) ) {
            #print "OK $e->{item_number}\n";
            $g_log->info(" OK ---> style $e->{style_id} item no $e->{item_number}");
        } else {
            #print "Missing $e->{style_id} $e->{item_number}\n";
            $g_log->info(" MISSING style $e->{style_id} item no $e->{item_number}");
            push(@z, $e);
        }
    }

    # Sort by style, color, size, dim
    # ... Nice!
    my @sorted = sort { $a->{style_id}     <=> $b->{style_id}  or
                        $a->{color_id}     cmp $b->{color_id}  or
                        $a->{size_id}      cmp $b->{size_id}   or
                        $a->{dimension_id} cmp $b->{dimension_id}
                      } @z;
    
    # Building the HTML body for email
    my $prev;
    my $html = '';
    my $n = 0;
    foreach my $i (@sorted) {
        $n++;
        $html .= "<tr>\n";
        $html .= qq(    <td align="right" style="color: orange;">$n</td>\n);
        $html .= "    <td>$i->{bar_code_id}</td>\n";
           $html .=  "    <td>$i->{style_id}</td>\n";

        #if ( $prev ne $i->{style_id} ) {
        #   
        #   $html .=  "    <td>$i->{style_id}</td>\n";
        #} else {
        #   $html .=  '    <td>&nbsp;</td>'."\n";
        #}

        $html .=  "    <td>$i->{color_id}</td>\n"
                    ."    <td>$i->{size_id}</td>\n"
                    ."    <td>$i->{dimension_id}</td>\n"
                    ."    <td>$i->{operation_date}</td>\n"
                    # ."    <td>$i->{activity_type}</td>\n"
                    ."</tr>\n";
        $prev = $i->{style_id};
    }

    if (@sorted) {
       # It means we have missing record
       # so let's send email
       
       $html = qq(<tr>\n) 
               . qq(<th>&nbsp;</th>\n) 
               . qq(<th>Barcode</th>\n) 
               . qq(<th>Style</th>\n) 
               . qq(<th>Color</th>\n) 
               . qq(<th>Size</th>\n) 
               . qq(<th>Dim</th>\n) 
               . qq(<th>Date Added</th>\n) 
               # . qq(<th>Operation Type</th>\n) 
               . qq(</tr>\n) 
               . $html;

       $html = qq(<table class="e832_table_nh">\n) . $html . "</table><p>program: $0</p><br>\n";

       my $subject = qq(Barcodes Added after WMS Item Interface);
       my $msg = <<ENDMSG;
<p>The following barcodes were not sent to HighJump because they were added after the Item Interface was run. 
<br>Please re-touch these styles to resend them.
</p>

ENDMSG
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
    get_ba_rec();



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
