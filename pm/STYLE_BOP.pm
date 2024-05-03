package MCCS::SAS::Loads::STYLE_BOP;
use strict;
use base qw(MCCS::SAS::Loads::Base);
use MCCS::SAS::MerchandiseLevelRecord;
use MCCS::SAS::Characteristics;
use MCCS::SAS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    my $db   = $util->get_database();
    my $sth  = $db->prepare(
        "UPDATE sas_product_master SET style_ind = '1' WHERE style_ind IS NULL AND style_id = ?"
    );
    $self->{'sth_update_product_master'} = $sth;
  	$self->{'char_obj'} = MCCS::SAS::Characteristics->new( $db );
}

sub get_sql {
    my $self = shift;
#   "SELECT 
#     sty.style_id, 
#     sty.description, 
#     sty.vendor_id,
#	 sas_utils.get_season_description(sty.season_id) season_desc,
#     nvl(sty.LAST_RECEIPT_DATE, to_date('19000101','YYYYMMDD') ), 
#     get_permanent_retail_price(30,null,sty.style_id,null,null,null,sysdate,null) retail,
#     sty.vendor_style_no
#	 from styles sty 
#	 where 
#     sty.date_created >= to_date('29-DEC-11', 'DD-MON-YY') 
#	 
#";
"SELECT 
     sty.style_id, 
     sty.description, 
     sty.vendor_id,
	 sas_utils.get_season_description(sty.season_id) season_desc,
     nvl(sty.LAST_RECEIPT_DATE, to_date('19000101','YYYYMMDD') ), 
     get_permanent_retail_price(30,null,sty.style_id,null,null,null,sysdate,null) retail,
     sty.vendor_style_no
	FROM 
        (select style_id 
           from styles ss where ss.date_created >= to_date('01-jan-12','dd-mon-yy')
           and not exists (select 1 from maxdata.lv7cmast\@eric_sasprd where lv7cmast_userid = ss.style_id)) base
	  join styles sty on (sty.style_id = base.style_id)
";
}

sub sql_new_products {
    q[SELECT 1 FROM sas_product_master WHERE  style_id = styles.style_id AND style_ind IS NULL];
}

sub sql_style_activity {
    q[SELECT 1 FROM style_colors_activity WHERE business_unit_id = 30 AND style_id = styles.style_id AND activity_type <> 'D' and creation_date between trunc((sysdate -3)) and sysdate];
}

sub sql_style_characteristics {
    q[SELECT 1 FROM style_characteristics_activity WHERE business_unit_id = 30 AND style_id = styles.style_id AND activity_type <> 'D' and creation_date between trunc((sysdate -3)) and sysdate];
}

sub make_record {
    my $self = shift;
    my ( $style, $name, $vendor, $season, $last_receipt_date, $perm_price, $vendor_style_id ) = @_;
    $name =~ s/[^[:print:]]//g;
    $name =~ s/\s+$//;

  	my %chars = $self->{'char_obj'}->get_sas_characteristics($style);
     my $clgrp = $chars{'user_attrib11'};
    my $obj = $self->{util}->_merchandise_level_record( $style, $name );
    $vendor_style_id =~ s/[\000-\037]//g;
   $obj->set(
        {   date_user7    => MCCS::SAS::DateTime->new($last_receipt_date),
            user_attrib11 => $vendor,
            user_attrib13 => $season,
            user_attrib16 => $perm_price,
            char_user12   => $vendor_style_id,
  			%chars,
            user_attrib38 => $clgrp
            
        }
    );

    #HJ  $self->product_update($style);

    $obj->to_string();
}

sub product_update {
    my $self  = shift;
    my $style = $_[0];
    $self->{'sth_update_product_master'}->execute($style);
}

sub get_filename {
    'MERCH_7_' . time() . '.txt';
}

sub site_field    { }
sub week_limiting { 0; }
sub database      { 'rms_p'; }

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::STYLE - MCCS::SAS STYLE hiearchy record extract

=head1 SYNOPSIS

MCCS::SAS::STYLE->new( MCCS::SAS::Util );

=head1 DESCRIPTION

This plugin extracts the data necessary for the style hiearchy in MCCS::SAS. This is a daily/weekly tracking load.

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>
Larry Lewis  L<larry.d.lewis@usmc-mccs.org|mailto:larry.d.lewis@usmc-mccs.org>

=cut
