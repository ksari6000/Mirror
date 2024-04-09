#!/usr/local/mccs/perl/bin/perl

use strict;
use MCCS::SPS::Util; 
use IBIS::DBI;
use IBIS::Log::File;
use IBIS::SFTP;
use Net::SFTP;
use MCCS::Config;
use Getopt::Long;
use Date::Manip;
use Data::Dumper;
use Pod::Usage;
use Class::Inspector;
use File::Spec;
use File::Basename;


use MCCS::SPS::Loads::Base;

use constant DBNAME => 'rms_p';
use constant DEFAULT_OUTPUT_DIR => '/usr/local/mccs/data/sps/';
my $cfg = new MCCS::Config;
my $sftpHash = $cfg->SpsDataLoad->{sftp};
#Readonly my $FMS_SERVER      => $cfg->banking->{DATASRC};  # fmsdev vs fmsprod


my %recCounts;
my $dir;
#my $type;
my @types = ();
my $date;
my $today;
my $week_ending;
my $debug;
my $database;
my $log = "/usr/local/mccs/log/sps/sps_data_$$.log";
my $help;
my $limit_records = 0;
my $suppress_send = 0;
my $archive_sw = 0;
my $markername;
my $host      = '';
my $remotedir = '';

#handle command line arguments
my $options = (GetOptions(
	'dir=s'      => \$dir,
    "type=s"     =>  \@types,
	'debug'      => \$debug,
	'database=s' => \$database,
	'nosend'     => \$suppress_send,
	'archive'     => \$archive_sw,
	'week_ending=s'  => \$week_ending,
	'help'       => \$help
	)
);
my $badmsg;

#--Get Dir by using PackageName.  Grep dir for valid FileTypes --#
my $plugin_dir = dirname( Class::Inspector->loaded_filename('MCCS::SPS::Loads::Base') );
my $dirfh;
opendir($dirfh, $plugin_dir) or die "Can not open Plugin Directory";
my @valid_types = 
	map{ s/\.pm//; $_ }
	grep{ /(\.pm)$/ && $_ ne 'Base.pm' }
	readdir($dirfh);
closedir($dirfh);
#--End Get Dir --#

my $activity = 'ACTIVITY';
#-- Handle Command Line Args --#
my $type_str = join ' ', @types;

if(
   $badmsg ||
   $help ||
   ! $options ||
   ! (grep{ $type_str =~ /$_/ } @valid_types) ||
   ((grep /$activity/, @types) && ! $week_ending)
  ){
        my $msg = $badmsg;
        if(! $options){
                $msg = 'Bad arguments';
        } elsif ((grep /$activity/,  @types) && ! $week_ending) {
                $msg = 'Week Ending Date need when type is ACTIVITY';
        } elsif(! $help){

		$msg = 'Bad type argument';
	}
	
	pod2usage(-noperldoc=>1, -verbose => 2, -msg => $msg);
}
#--END handle command line arguments

#logging
my $log_obj = IBIS::Log::File->new( {file=>$log,append=>1} );
mylog("Started");
mylog("Logfile: ", $log);
mylog("Record limit: ", $limit_records) if $limit_records;


my $fhdir = (! $dir) ? DEFAULT_OUTPUT_DIR : $dir;

my $date = `date '+%Y%m%d'`;
chomp($date);


my @fileNames;
  
#-- Loop thru data types --#
foreach my $type (@types) {

   my $type_class = 'MCCS::SPS::Loads::' . $type;
   eval("use $type_class;"); die $@ if $@;
   mylog("Output type: ", $type);
   my $count=0;
   my $gfile = $type_class->get_filename($week_ending);
   push(@fileNames, $gfile);
      
   my $file = (! $dir) ? File::Spec->catfile(DEFAULT_OUTPUT_DIR, $gfile) : qq($dir/$gfile);
   $database = $database || $type_class->database() || DBNAME;
   
   mylog("Database: ", $database);
   mylog("Output file: ", $file);

   my $sps_util = MCCS::SPS::Util->new($database, $file );	#automatically write to file path
   my $db = $sps_util->get_database();
   my $type_obj = $type_class->new( $sps_util );
   my $table = 'sps_activity_'. $week_ending if ($type == 'ACTIVITY'); 
   #--Get SQL from Load::Package --#
   my $sql = sprintf($type_obj->get_sql(), $table );

   debug('Type SQL: ',$sql);
   
   my $sth = $db->prepare($sql);
   $sth->execute();	#no merch date filter
   
   $type_obj->make_header( ); 
   while(my $myrow = $sth->fetchrow_arrayref()){
	  my $make_ret = $type_obj->make_record( @{ $myrow } );
	  $count++ if defined $make_ret;
	  if($limit_records && $count >= $limit_records){
		last;
	   }
    }

    $type_obj->finish();
    $db->commit();

    mylog("Completed $count records");

  END {
    $type_obj = undef;
    if($sth){ $sth->finish(); }
    if($db){ $db->disconnect(); }
    
  }
    
}
 
use File::Copy;
  
chdir('/usr/local/mccs/data/sps/');

my @sftpFiles;
foreach my $val (@fileNames) {
	my $zipname   = $val;
	$zipname =~ s/dat/zip/; 
	push(@sftpFiles, $zipname);
    system("zip -m $zipname $val" );    
}

ftpFiles();

archiveFiles();  



sub archiveFiles {
	if ( $archive_sw ) {
	  my $date = `date '+%Y%m%d'`;
      chomp($date);
	  my $arc = "/usr/local/mccs/data/sps/archive/".$date;

      mkdir($arc) if (!-e "$arc");
      foreach (@sftpFiles) {
         system("mv /usr/local/mccs/data/sps/$_ $arc" );
      }
	}
      	
}


sub ftpFiles {


if ( !$suppress_send ) {
    my $sftp;
    my %arglist = (
        user => $sftpHash->{user},
        password => $sftpHash->{password},
        debug => $debug,
    );

    my $maxresend = 3;
    while ( $maxresend-- ) {
            eval { $sftp = Net::SFTP->new( $sftpHash->{host}, %arglist ) };
            if ( !$@ ) {$maxresend++;  last;}
            mylog("Connection Failed... $maxresend Attempts left to connect to SPS.. Retrying!\n");
            sleep(10);
    }

    if ($maxresend > 0) {
        foreach my $val (@sftpFiles) {
            $sftp->put($val, 'INBOX/'.$val);
            mylog("SFTP PUT of $val to SPS server failed!") if ($sftp->status);
        }
    } else {
        my $msg = "FAIL to Connect to SPS to $host : $remotedir";
        die $msg;
    }

} else {
    mylog("Skipping file send per command line switch --nosend\nFiles to be sent @sftpFiles");
}

}


sub mylog{
	my $log_entry = join('',"(PID $$) ",@_);
	if($log_obj){ $log_obj->info( $log_entry ); }
	debug($log_entry);
}



sub debug{
	if($debug){
		print "DEBUG: ", @_, "\n";
	}
}

__END__

=pod

=head1 NAME

perl sps_data_load.pl --type LOCATION  --database rms_r --debug --archive --nosend
perl sps_data_load.pl --type PRODUCT  --database rms_r --debug --archive --nosend
perl sps_data_load.pl --type ACTIVITY  --week_ending '20140301'--database rms_p --debug --archive --nosend

=head1 SYNOPSIS

spss_data_load.pl
--DIR [output DIR path]
--type []
--database [optional database]
--help
--debug

=head1 DETAILS

=head1 DESCRIPTION

TO be done

=head1 DEPENDENCIES

=over 4

=item MCCS::SAS::Util

Is used to create the various types of SAS records necessary to accurately output the correct files.

=back

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
