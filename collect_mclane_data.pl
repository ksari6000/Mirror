#!/usr/local/mccs/perl/bin/perl
#------------------------------------------------------------------------
# Ported by: Hanny Januarius
# Date: Thu Dec  7 10:52:04 EST 2023
# Desc:
#   Connect to rms_r DB
#   Invoke stored procedure to create MClane report file
#   Create Flat file to send to MClane
#   Send daily file to MClane
#   Run stored procedure to create MClane report work file
#   Create daily flat file to send to Mcal
#   SFTP file to MCL
# Updated by:  Kaveh Sari 4/12/2024
# Ensured directories persent, updated email to Kav ONLY.  
#------------------------------------------------------------------------
use strict;
use IBIS::DBI;
use DateTime;
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
Readonly my $g_logfile => '/usr/local/mccs/log/mclane/'
  . basename(__FILE__) . '.log';

my $crt_mclane_data = <<CRTMCLRPTFILE;
Begin MRI_BUILD_MCLANE_DATA(:NumOfDays); End;
CRTMCLRPTFILE

my $stage;
my $retry;
my $g_weekEndingDate;
use constant DEFAULT_OUTPUT_DIR => '/usr/local/mccs/data/mclane/';

# Default verbosity mode
my $g_verbose = 0;

# Instantiate MCCS::Config object
my $g_cfg = new MCCS::Config;

### CREATE THESE CONFIGURATION ENTRIES IN IBISCFG.XML FOR MCLANE

my $g_NumOfDays = $g_cfg->mcl_DATA->{NumOfDays};
#my $g_emails = $g_cfg->mcl_DATA->{emails};
my $g_emails->{kav}  = 'kaveh.sari@usmc-mccs.org';
my $g_email_sub = $g_cfg->mcl_DATA->{email_sub};
( my $g_mclFile = $g_cfg->mcl_DATA->{FILE_TO_PRODUCE} ) =~ s/YYYYMMDD/my $x = `date +%Y%m%d`; chomp $x; $x/e;
  
# Extract list of sites to report for MCL from configuration file
#Updated to have parenthesis around arguments to the split function, due to 
#the parser interpreting the line incorrectly. KS
my @g_rpt_sites = map {"'" . $_ . "'"} split (/\|/, $g_cfg->mcl_DATA->{rpt_sites}); 
  
# Initialize variable with current date (as in mm/dd/yy hh:mn:ss AM/PM)
my $g_long_date = `date +"%D %r"`;
chomp($g_long_date);

# Initialize variable with short date for Archiving purposes
my $g_date = `date +%F`;
chomp($g_date);

# Instantiate log file object
my $g_log =
  IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );

#handle command line arguments  (ANY NEW OPTION NEEDED?)
##my $options = ( GetOptions( 'stage=s' => \$stage, 'retry=s' => \$retry ) );

# Connect to DB
my $g_dbh = IBIS::DBI->connect( dbname => 'rms_r' );

# Invoke stored procedure to create MClane report file
create_MCL_Rpt_Table();

# Create Flat file to send to MClane
createFlatFile();

# Send daily file to MClane
sftpTOMCL();

# Run stored procedure to create MClane report work file
sub create_MCL_Rpt_Table {
	my $sth;
	$g_log->info('Main sproc started execution' );
	eval { $sth = $g_dbh->prepare($crt_mclane_data) };
	if ($@) {
		fatal_error("Failed to prepare query below:\n$crt_mclane_data\n");
	}

	eval {
		$sth->bind_param( ":NumOfDays", $g_NumOfDays );
		$sth->execute();
	$g_log->info('Main sproc completed execution' );	
	};

	if ($@) {
		fatal_error(
			"Failed while attempting to execute procedure $crt_mclane_data: ($@)\n");
	}
}

# Create daily flat file to send to Mcal
sub createFlatFile {
	my $flatFileData = <<MCLFLATFILE;
select        site_id,
              upc,
              avg_pos_qty,
              act_pos_qty,
              curr_qty_onhand
FROM          mri_mclane_rpt_file
MCLFLATFILE

# Execute SQL above to create a flat file with the 90 day rolling metrics required by MClane
    $g_log->info('Flat file creation started' );
    
# Determine if specific sites must be selected
# Add on 'where' clause if list of sites contains at least 1 site
    $flatFileData .= ' where site_id in (' . (join ',',@g_rpt_sites) . ')' if @g_rpt_sites; 
    
	my $sth = $g_dbh->prepare($flatFileData);
	$sth->execute();

	# Create flat file to be sent to MCL
	open OUT, '>', DEFAULT_OUTPUT_DIR . $g_mclFile;

	# Write records to flat file
	my $row;
	while ( $row = $sth->fetchrow_hashref() ) {
		print OUT $row->{site_id} . ',';
		print OUT $row->{upc} . ',';
		print OUT $row->{avg_pos_qty} . ',';
		print OUT $row->{act_pos_qty} . ',';
		print OUT ( $row->{curr_qty_onhand} < 0 ? 0 : $row->{curr_qty_onhand} )
		  . "\n";
	}

	# close flat file
	close OUT;
	$g_log->info('Flat file creation completed' );
}

# SFTP file to MCL
sub sftpTOMCL {

	# Define argument list needed by NET:SFTP
	my %arglist;

	# Perform steps here ONLY if MCL flat file exists
	if ( -e DEFAULT_OUTPUT_DIR . $g_mclFile ) {

		# Retrieve destination server and directory
		my $dest     = $g_cfg->mcl_DATA->{FTP_SERVER};
		my $remote_d = $g_cfg->mcl_DATA->{REMOTE_DIR};

		# Retrieve MCL user name and password
		$arglist{user}     = $g_cfg->mcl_DATA->{USER};
		$arglist{password} = $g_cfg->mcl_DATA->{PSWD};
                $arglist{more}     = '-v';

		# Log server name and directory
		$g_log->info('SFTP transfer started' );
		$g_log->info("FTP_SERVER: $dest");
		$g_log->info("REMOTE_DIR: $remote_d");

		# Establish SFTP connection to MCL server
		my $sftp;
		my $num_retry      = 10;
		my $successful_ftp = 'N';
		my $attempt;
		while ( $num_retry-- ) {
			eval { $sftp = Net::SFTP::Foreign->new( $dest, %arglist ) };
			if ( !$@ ) { $successful_ftp = 'Y'; last }
			$attempt = 10 - $num_retry;
			$g_log->info("Attempt $attempt to connect to $dest failed!\n");
			sleep(10);
		}

		if ( $successful_ftp eq 'N' ) {
			fatal_error("SFTP connection to MCL server ($dest) failed!");
		}
		$sftp->put(
			DEFAULT_OUTPUT_DIR . $g_mclFile, $remote_d . $g_mclFile,
			copy_perms => 0,
			copy_time  => 0,
			atomic     => 1
		);
		fatal_error("$g_mclFile could not be sent") if ( $sftp->error );
   	        $g_log->info("file $g_mclFile has been sftp PUSHED to $remote_d dir");

	}
	else {
		fatal_error( 'MCL flat file '
			  . DEFAULT_OUTPUT_DIR
			  . $g_mclFile
			  . ' does not exist. Alert RDI personnel!' );
	}

   	$g_log->info('SFTP transfer completed' );
	# Archive succesfully sent file
	archiveSentFile();
}

#  Routine to archive produced and sent files
sub archiveSentFile {

	# Retrieve name of MCL Archive directory
	my $arc_dir = $g_cfg->mcl_DATA->{ARC_DIR};

	# Create archive directory
	if ( !-d "$arc_dir/$g_date" ) {
		mkpath("$arc_dir/$g_date")
		  or fatal_error("Creation of directory $arc_dir/$g_date has failed!");
	}

	# Archive MCL file
	copy( DEFAULT_OUTPUT_DIR . $g_mclFile, "$arc_dir/$g_date/$g_mclFile" )
	  or fatal_error(
		"Archiving of " . DEFAULT_OUTPUT_DIR . $g_mclFile . ' failed!' );
	# Send email confirming that process ended normally
	send_mail( 'MCL EXTRACT',
		' EXTRACT FILE HAS BEEN SUCCESFULLY SENT TO MCL SERVER ' );
}

##################################################################################
#      Routine to send/email errors and croak
##################################################################################
sub fatal_error {
	my $msg = shift;
	send_mail( $g_email_sub, $msg );
	$g_log->info($msg);
	croak($msg);
}
##################################################################################
#      Routine to send notification emails
##################################################################################
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

