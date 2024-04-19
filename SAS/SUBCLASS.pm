package MCCS::SAS::Loads::SUBCLASS;
use strict;
use base qw(MCCS::SAS::Loads::Base);
use MCCS::SAS::MerchandiseLevelRecord;

use constant SQL => "
	/*
		sub_class_id means nothing in of itself (and will repeat).select voucher_id, reference_1 from ap_vouchers where company_id = 'H09' AND create_date > sysdate - 1 AND comment_text LIKE 'SITE%'

		In this case use section_id as unique key for subclass (being the unique combination of department/class/subclass)
	*/
	SELECT distinct department_id || '_' || class_id || '_' || sub_class_id, lower( MAX(sub_class_descr) )
	from v_dept_class_subclass
	WHERE business_unit_id = 30 and sub_class_descr IS NOT NULL AND sub_class_id IS NOT NULL
  	GROUP BY department_id, class_id, sub_class_id
  	union
  	SELECT distinct department_id || '_' || class_id || '_NULL'  , 'other ' || lower( MAX(dept_name) )
	from v_dept_class_subclass
	WHERE business_unit_id = 30 and sub_class_id IS  NULL
  	GROUP BY department_id, class_id, sub_class_id
  	
";

sub get_sql{
	SQL;
}

sub make_record{
	my $self = shift;
	$self->{util}->_merchandise_level_record(@_)->to_string();
}

sub get_filename{
	'MERCH_6_' . time() . '.txt';
}

sub site_field{ }
sub week_limiting {0;}

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::SUBCLASS - MCCS::SAS SUBCLASS hiearchy record extract

=head1 SYNOPSIS

MCCS::SAS::SUBCLASS->new( MCCS::SAS::Util );

=head1 DESCRIPTION

This plugin extracts the data necessary for the subclass hiearchy in MCCS::SAS. This is a full load (i.e. all subclasses).

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut