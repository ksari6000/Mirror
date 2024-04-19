package MCCS::SAS::Loads::LOB;
use strict;
use base qw(MCCS::SAS::Loads::Base);
use MCCS::SAS::MerchandiseLevelRecord;

use constant SQL => "
	SELECT lobid, lobdesc,
		(
 SELECT
   e.last_name
   FROM
   departments d,
      employees e
    WHERE
     d.business_unit_id = 30 AND
     d.business_unit_id = e.business_unit_id AND
     d.buyer_employee_id = e.employee_id
      AND d.department_id = inn.deptid
		) buyer
	FROM (
		select  lobid, lobdesc, MIN(deptid) deptid from te_dept_hier GROUP BY lobid, lobdesc
	) inn
";

sub get_sql {
    SQL;
}

sub make_record {
    my $self = shift;
    my ( $id, $name, $buyer ) = @_;
    my $obj = $self->{util}->_merchandise_level_record( $id, $name );
    $obj->set( { user_attrib1 => $buyer } );
    $obj->to_string();
}

sub get_filename {
    'MERCH_3_' . time() . '.txt';
}

sub site_field    { }
sub week_limiting { 0; }

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::LOB - MCCS::SAS LOB hiearchy record extract

=head1 SYNOPSIS

MCCS::SAS::LOB->new( MCCS::SAS::Util );

=head1 DESCRIPTION

This plugin extracts the data necessary for the lob hiearchy in MCCS::SAS. This is a full load (i.e. all lobs).

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
