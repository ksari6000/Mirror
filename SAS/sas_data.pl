#!/usr/bin/perl
# SAS Planning and Assortment data
use strict;
use MCCS::SAS::Util; 
use IBIS::DBI;
use IBIS::Log::File;
use Getopt::Long;
use Date::Manip;
use Data::Dumper;
use Pod::Usage;
use File::Spec;
use File::Basename;
use Class::Inspector;

use MCCS::SAS::Loads::Base;
use MCCS::SAS::Pre_Reclass_Utils;

use constant DBNAME => 'rms_p';
use constant DEFAULT_OUTPUT_DIR => '/usr/local/mccs/data/sas/';
use constant DEFAULT_OUTPUT_DIR_PRCLS => '/usr/local/mccs/data/sas/pre_reclass/';


use constant FIND_THIS_MERCH_WEEK => "SELECT get_merch_year, get_merch_week FROM dual ";

my @sites = ();
my $file;
my $type;
my $date;
my $today;
my $debug;
my $database;
my $log = "/usr/local/mccs/log/sas_data_$$.log";
my $merchyear;
my $merchweek;
my $merchweek2;
my $help;
my $count=0;
my $limit_records = 0;
my $pre_rcls;

#handle command line arguments
my $options = (GetOptions(
	'site=s' => \@sites,
	'file=s' => \$file,
	'type=s' =>  \$type,
	'debug' => \$debug,
	'database=s' => \$database,
	'merchyear=i' => \$merchyear,
	'merchweek=i' => \$merchweek,
	'limit_records=i' => \$limit_records,
	'log=s' => \$log,
	'help' => \$help,
	'pre_rcls' => \$pre_rcls
	)
);
my $badmsg;

if( ($merchyear && ! $merchweek) || ($merchweek && ! $merchyear) ){
	$badmsg = 'Both Merchant Year and Merchant Week need to be defined';
}

my $plugin_dir = dirname( Class::Inspector->loaded_filename('MCCS::SAS::Loads::Base') );
my $dirfh;
opendir($dirfh, $plugin_dir) or die "Can not open Plugin Directory";
my @valid_types = 
	map{ s/\.pm//; $_ }
	grep{ /(\.pm)$/ && $_ ne 'Base.pm' }
	readdir($dirfh);
closedir($dirfh);
print @valid_types;