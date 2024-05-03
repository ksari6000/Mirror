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

if($badmsg || $help || ! $options || ! grep{ $type eq $_ } @valid_types
){
	my $msg = $badmsg;
	if(! $options){
		$msg = 'Bad arguments';
	} elsif(! $help){
		$msg = 'Bad type argument'
	}
	pod2usage(-noperldoc=>1, -verbose => 2, -msg => $msg);
}
# END handle command line arguments

#logging
my $log_obj = IBIS::Log::File->new( {file=>$log,append=>1} );
mylog("Started");
mylog("Sites: ", join(',', @sites));
mylog("Output type: ", $type);
mylog("Logfile: ", $log);
mylog("Merch Year: ", $merchyear);
mylog("Merch Week: ", $merchweek);
mylog("Record limit: ", $limit_records) if $limit_records;

my $type_class = 'MCCS::SAS::Loads::' . $type;
eval("use $type_class;"); die $@ if $@;

if(! $file){
	##my $type_obj_tmp = $type_class->new( );	#this instance of type_obj will only be used to get the filename of the class type
	my $gfile = $type_class->get_filename($merchyear,$merchweek);
	$file = File::Spec->catfile($pre_rcls? DEFAULT_OUTPUT_DIR_PRCLS : DEFAULT_OUTPUT_DIR, $gfile);
}

$database = $database || $type_class->database() || DBNAME;
mylog("Database: ", $database);
mylog("Output file: ", $file);

my $sas_util = MCCS::SAS::Util->new($database, $file );	#automatically write to file path
my $db = $sas_util->get_database();
my $type_obj = $type_class->new( $sas_util );

#The data needs filtered to a merchandising year/week
if($type_obj->week_limiting()){
	if(! $merchyear){	#merch dates not passed in, use current merch year/week
		my $date_sth = $db->prepare( FIND_THIS_MERCH_WEEK );
		$date_sth->execute();
		($merchyear,$merchweek) = $date_sth->fetchrow_array();
	}
}

#form SQL
my $sql = sprintf($type_obj->get_sql(), make_site_in(\@sites, $type_obj->site_field()) );
debug('Type SQL: ',$sql);
#execute SQL and feed results to output record format
my $sth = $db->prepare($sql);

if($type_obj->week_limiting()) {
	    $sth->execute($merchyear,$merchweek);	#SQL will have a merch date filter
} else {
	$sth->execute();	#no merch date filter
}

while(my $myrow = $sth->fetchrow_arrayref()){
#	my $routine = $define{$type}->{'routine'};
	my $make_ret = $type_obj->make_record( @{ $myrow } );
#	$sas_util->$routine( @{ $myrow } )->to_string();
	$count++ if defined $make_ret;
	if($limit_records && $count >= $limit_records){
		last;
	}
}
$type_obj->finish();
$db->commit();

# If pm has been called for Pre_reclass processing (pre_rcls flag on ) and MERCH10, populate MRI_ORDER_CODES
if ($pre_rcls && $type eq 'FULL_MERCH10_LOAD_RECLASS') {
	my $pre_reclass =  MCCS::SAS::Pre_Reclass_Utils->new(pre_reclass_log=>$log_obj, email => 'rdistaff@usmc-mccs.org');
	$pre_reclass->populate_mri_order_codes($file);
}

END{
	$type_obj = undef;
	if($sth){ $sth->finish(); }
	if($db){ $db->disconnect(); }
	mylog("Completed $count records");
}

sub make_site_in {
	my $sites = shift;
	my $field_name = shift || 'site_id';
	
	#form optional sites in clause
	my $site_in='';
	if(ref($sites) && @$sites){
		my @sites = @{ $sites };
		#if the sites paramter happens to be a file path
		if(@sites == 1 && $sites[0] =~ /\D/){
			my $site_fh;
			if(! open($site_fh, $sites[0])){
				my $msg = "Can not open $sites[0]";
				mylog($msg);
				die $msg;
			}
			@sites = <$site_fh>;	#get my list of sites (one per line)
			close($site_fh);
			map{ chomp; } @sites;	#strip off ending new line
			mylog("Sites read from file: ", join(',',@sites));
		}
		#assemble SQL IN clause
		$site_in = " AND $field_name IN (%s)";
		my $site_str = join("', '", @sites);	$site_str = "'" . $site_str . "'";
		$site_in = sprintf($site_in, $site_str);
	}
	$site_in;
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

sas_data.pl

=head1 SYNOPSIS

sas_data.pl
--file [output file path]
--type [DIVISION | LOB | DEPARTMENT | CLASS | SUBCLASS | STYLE | PRODUCT | LOCATION | SALE | INVENTORY | ONORDER]
--site [optional site(s)]
--database [optional database]
--merchyear [optional merchandising year]
--merchweek [optional merchandising week]
--help
--debug

=head1 DETAILS

=head1 DESCRIPTION

SAS data extract. There are different types of data to extract from RMS for the SAS Planning and Assortment application.
The extract types use a plugin architecture where the module for extracting lives in the MCCS::SAS::Loads package and inherits from
MCCS::SAS::Loads::Base.

Some extracts take a merchandising year and week (merchyear, merchweek).

=head1 DEPENDENCIES

=over 4

=item MCCS::SAS::Util

Is used to create the various types of SAS records necessary to accurately output the correct files.

=back

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
