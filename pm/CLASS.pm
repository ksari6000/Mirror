package MCCS::SAS::Loads::CLASS;
use strict;
use base qw(MCCS::SAS::Loads::Base);
use MCCS::SAS::MerchandiseLevelRecord;

use constant SQL => "
	/*
		class id means nothing by itself (and will repeat)... 
        only in combination with department...
		therefore, derive a a key based on the combination of 
        department_id and class_id
	*/
	SELECT department_id || '_' || class_id myid, 
           initcap(lower(MIN(class_descr))) d
	FROM   v_dept_class_subclass
	WHERE  class_descr IS NOT NULL
  GROUP BY department_id, class_id
";

sub get_sql {
    SQL;
}

sub make_record {
    my $self = shift;
    $self->{util}->_merchandise_level_record(@_)->to_string();
}

sub get_filename {
    'MERCH_5_' . time() . '.txt';
}

sub site_field    { }
sub week_limiting { 0; }

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::CLASS - MCCS::SAS CLASS hiearchy record extract

=head1 SYNOPSIS

MCCS::SAS::CLASS->new( MCCS::SAS::Util );

=head1 DESCRIPTION

This plugin extracts the data necessary for the class hiearchy in MCCS::SAS. This is a full load (i.e. all classes).

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
