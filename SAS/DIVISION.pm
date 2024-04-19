package MCCS::SAS::Loads::DIVISION;
use strict;
use base qw(MCCS::SAS::Loads::Base);
use MCCS::SAS::MerchandiseLevelRecord;

use constant SQL => "
	select distinct divid, divdesc from te_dept_hier
";

sub get_sql {
    SQL;
}

sub make_record {
    my $self = shift;
    $self->{util}->_merchandise_level_record(@_)->to_string();
}

sub get_filename {
    'MERCH_2_' . time() . '.txt';
}

sub site_field    { }
sub week_limiting { 0; }

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::DIVISION - MCCS::SAS DIVISION hiearchy record extract

=head1 SYNOPSIS

MCCS::SAS::DIVISION->new( MCCS::SAS::Util );

=head1 DESCRIPTION

This plugin extracts the data necessary for the division hiearchy in MCCS::SAS. This is a full load (i.e. all divisions).

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
