package MCCS::SAS::Loads::DEPARTMENT;
use strict;
use base qw(MCCS::SAS::Loads::Base);
use MCCS::SAS::MerchandiseLevelRecord;

use constant SQL => "
	select distinct department_id, upper(dept_name) from v_dept_class_subclass WHERE length(dept_name) > 0 and business_unit_id = '30'
";

sub get_sql {
    SQL;
}

sub make_record {
    my $self = shift;
    $self->{util}->_merchandise_level_record(@_)->to_string();
}

sub get_filename {
    'MERCH_4_' . time() . '.txt';
}

sub site_field    { }
sub week_limiting { 0; }

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::DEPARTMENT - MCCS::SAS DEPARTMENT hiearchy record extract

=head1 SYNOPSIS

MCCS::SAS::DEPARTMENT->new( MCCS::SAS::Util );

=head1 DESCRIPTION

This plugin extracts the data necessary for the department hiearchy in MCCS::SAS. This is a full load (i.e. all departments).

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
