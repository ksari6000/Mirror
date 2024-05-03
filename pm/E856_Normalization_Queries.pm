#------------------------------------------------------------------------
#Queries used in the Saving of parsed EDI 856 DATA to 'rdiusr' schema
# 
#------------------------------------------------------------------------
package MCCS::DBI::E856_Normalization_Queries;

use strict;
use Carp;
use base qw(IBIS::xController::DataAccessor);
use IBIS::Log::File;
use Data::Dumper;
use Time::localtime;
my $pkg = __PACKAGE__;

#--------------------------------------------------------------------------
our (@EXPORT_OK, %EXPORT_TAGS);

@EXPORT_OK = qw();

%EXPORT_TAGS = (
    ALL => [ @EXPORT_OK ],
    );

#--------------------------------------------------------------------------
# FYI what you cant find here search what is qualified in the 'base' 
# and what you can't find there search it's base.  
#                          'base-ception'
#--------------------------------------------------------------------------

sub get_record {
    my $self = shift;
    my %params = @_;
        
    for my $key (keys %params) {
        $self->{$key} = $params{$key} 
    }
    
    my $sql;      
    $sql = $self->get_next_ctrl_seq()    if ('getNextCtrlSeq' eq $self->{dataSource}) ;
    
    my $results;
    $results = $self->single_row_sql( $sql, \%params) if ('getNextCtrlSeq' eq $self->{dataSource}) ;  
    
    return $results;      
}

sub set_record {
	my $self = shift;
    my %params = @_;
        
    for my $key (keys %params) {
        $self->{$key} = $params{$key} 
    }
    
    my $sql;      
    $sql = $self->insert_ctrl_rec() if ('insCtrlRec' eq $self->{dataSource}) ;
    $sql = $self->insert_line_10()  if ('insLine10' eq $self->{dataSource}) ;
    $sql = $self->insert_line_20()  if ('insLine20' eq $self->{dataSource}) ;
    $sql = $self->insert_line_30()  if ('insLine30' eq $self->{dataSource}) ;
    $sql = $self->insert_line_40()  if ('insLine40' eq $self->{dataSource}) ;
    $sql = $self->insert_line_50()  if ('insLine50' eq $self->{dataSource}) ;
    $sql = $self->insert_line_60()  if ('insLine60' eq $self->{dataSource}) ;
    $sql = $self->insert_line_90()  if ('insLine90' eq $self->{dataSource}) ;
    
    my $results;
    $results = $self->bind_execute_sql( $sql, \%params) if ('insCtrlRec' eq $self->{dataSource}) ;  
    $results = $self->bind_execute_sql( $sql, \%params) if ('insLine10' eq $self->{dataSource}) ;
    $results = $self->bind_execute_sql( $sql, \%params) if ('insLine20' eq $self->{dataSource}) ;
    $results = $self->bind_execute_sql( $sql, \%params) if ('insLine30' eq $self->{dataSource}) ;
    $results = $self->bind_execute_sql( $sql, \%params) if ('insLine40' eq $self->{dataSource}) ;
    $results = $self->bind_execute_sql( $sql, \%params) if ('insLine50' eq $self->{dataSource}) ;
    $results = $self->bind_execute_sql( $sql, \%params) if ('insLine60' eq $self->{dataSource}) ;
    $results = $self->bind_execute_sql( $sql, \%params) if ('insLine90' eq $self->{dataSource}) ;
    
    return $results;   
}

#--------------------------------------------------------------------------#
#     Select Query Section
#--------------------------------------------------------------------------#
sub get_next_ctrl_seq {
    my $self = shift;
    my %params = %{$self};
      
    my $sql = qq(

select e856_ctrl_rec_seq.nextval from dual

    );

return $sql;
 }
  
#--------------------------------------------------------------------------#
#     Insert/Update Query Section
#--------------------------------------------------------------------------#

sub insert_ctrl_rec {
    my $self = shift;
    my %params = @_;
    
    #the order that the values will be place in the value bind
    @{$self->{ $self->{dataSource}."BindOrder" } } = ('record_id',
                                                      'partnership_id',
                                                      'asn_id',
                                                      'asn_date',
                                                      'asn_time',
                                                      'sps_file_name',
                                                      'created_date',
                                                      'created_time'
                                                      );
    
    my $sql = qq( 
insert into e856_ctrl_records  
  (record_id, partnership_id, asn_id, asn_date, asn_time, sps_file_name,
   created_date, created_time)
values (?,?,?,?,?,?,?,?)
    );
 }

sub insert_line_10 {
    my $self = shift;
    my %params = @_;
    
    #the order that the values will be place in the value bind
    @{$self->{ $self->{dataSource}."BindOrder" } } = ('record_id',
                                                      'partnership_id',
                                                      'record_type',
                                                      'trans_set_purpose',
                                                      'shipment_id',
                                                      'record_date',
                                                      'record_time',
                                                      'hier_structure_code'
                                                      );
    
    my $sql = qq( 
insert into e856_line_10  
  (record_id, partnership_id, record_type, trans_set_purpose, shipment_id, 
   record_date, record_time, hier_structure_code) 
values (?,?,?,?,?,?,?,?)
    );
 }

sub insert_line_20 {
    my $self = shift;
    my %params = @_;
    
    #the order that the values will be place in the value bind
    @{$self->{ $self->{dataSource}."BindOrder" } } = ('record_id',
                                                      'partnership_id',
                                                      'record_type',
                                                      'hier_lvl_id',
                                                      'hier_lvl_code',
                                                      'packaging_code',
                                                      'number_cartons',
                                                      'weight_qualifier',
                                                      'weight',
                                                      'weight_unit_measure',
                                                      'scac_carrier_code',
                                                      'bol_number',
                                                      'mstr_bol_number',
                                                      'pro_number',
                                                      'shipped_date',
                                                      'sched_delivery_date',
                                                      'ship_method_payment'
                                                      );
    
    
    my $sql = qq( 
insert into e856_line_20  
  (record_id, partnership_id, record_type, hier_lvl_id, hier_lvl_code,
   packaging_code, number_cartons, weight_qualifier, weight, weight_unit_measure,
   scac_carrier_code, bol_number, mstr_bol_number, pro_number, shipped_date,
   sched_delivery_date, ship_method_payment) 
values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    );
 }

sub insert_line_30 {
    my $self = shift;
    my %params = @_;
    
    #the order that the values will be place in the value bind
    @{$self->{ $self->{dataSource}."BindOrder" } } = ('record_id',
                                                      'partnership_id',
                                                      'record_type',
                                                      'ship_from_qualifier',
                                                      'ship_from_name',
                                                      'ship_from_assign_by',
                                                      'ship_from_code',
                                                      'ship_from_address',
                                                      'ship_from_city',
                                                      'ship_from_state',
                                                      'ship_from_zip_code',
                                                      'ship_to_qualifier',
                                                      'ship_to_name',
                                                      'ship_to_assign_by',
                                                      'ship_to_code',
                                                      'ship_to_address',
                                                      'ship_to_city',
                                                      'ship_to_state',
                                                      'ship_to_zip_code'
                                                      );
    
    my $sql = qq( 
insert into e856_line_30  
  (record_id, partnership_id, record_type, ship_from_qualifier, ship_from_name,
   ship_from_assign_by, ship_from_code, ship_from_address, ship_from_city, 
   ship_from_state, ship_from_zip_code, ship_to_qualifier, ship_to_name, 
   ship_to_assign_by, ship_to_code, ship_to_address, ship_to_city, ship_to_state,
   ship_to_zip_code)  
values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    );
 }


sub insert_line_40 {
    my $self = shift;
    my %params = @_;
    
    #the order that the values will be place in the value bind
    @{$self->{ $self->{dataSource}."BindOrder" } } = ('record_id',
                                                      'lvl_40_id',
                                                      'partnership_id',
                                                      'record_type',
                                                      'hier_lvl_id',
                                                      'hier_parent_id',
                                                      'hier_lvl_code',
                                                      'po_id',
                                                      'release_number',
                                                      'po_date',
                                                      'packaging_code',
                                                      'number_cartons',
                                                      'weight_qualifier',
                                                      'weight',
                                                      'weight_unit_measure',
                                                      'seller_invoice_num',
                                                      'entity_id',
                                                      'currency_code',
                                                      'mark_for_site'
                                                      );
    
    my $sql = qq( 
insert into e856_line_40  
  (record_id, lvl_40_id, partnership_id, record_type, hier_lvl_id, hier_parent_id,
   hier_lvl_code, po_id, release_number, po_date, packaging_code, number_cartons,
   weight_qualifier, weight, weight_unit_measure, seller_invoice_num, entity_id,
   currency_code, mark_for_site)  
values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    );
 }

sub insert_line_50 {
    my $self = shift;
    my %params = @_;
    
    #the order that the values will be place in the value bind
    @{$self->{ $self->{dataSource}."BindOrder" } } = ('record_id',
                                                      'lvl_40_id',
                                                      'lvl_50_id',
                                                      'partnership_id',
                                                      'record_type',
                                                      'hier_lvl_id',
                                                      'hier_parent_id',
                                                      'hier_lvl_code',
                                                      'mark_qualifier',
                                                      'carton_code'
                                                      );
    
    my $sql = qq( 
insert into e856_line_50  
  (record_id, lvl_40_id, lvl_50_id, partnership_id, record_type, hier_lvl_id,
   hier_parent_id, hier_lvl_code, mark_qualifier, carton_code) 
values (?,?,?,?,?,?,?,?,?,?)
    );
 }

sub insert_line_60 {
    my $self = shift;
    my %params = @_;
    
    #the order that the values will be place in the value bind
    @{$self->{ $self->{dataSource}."BindOrder" } } = ('record_id',
                                                      'lvl_40_id',
                                                      'lvl_50_id',
                                                      'lvl_60_id',
                                                      'partnership_id',
                                                      'record_type',
                                                      'hier_lvl_id',
                                                      'hier_parent_id',
                                                      'hier_lvl_code',
                                                      'bar_code_qualifier',
                                                      'barcode_id',
                                                      'buyer_style',
                                                      'buyer_color',
                                                      'buyer_size',
                                                      'vendor_style',
                                                      'nrf_color',
                                                      'nrf_size',
                                                      'quantity_shipped',
                                                      'qty_ship_unit_measure',
                                                      'quantity_ordered',
                                                      'qty_ordered_unit_measure'
                                                      );
    
    my $sql = qq( 
insert into e856_line_60  
  (record_id, lvl_40_id, lvl_50_id, lvl_60_id, partnership_id, record_type,
   hier_lvl_id, hier_parent_id, hier_lvl_code, bar_code_qualifier, barcode_id,
   buyer_style, buyer_color, buyer_size, vendor_style, nrf_color, nrf_size,
   quantity_shipped, qty_ship_unit_measure, quantity_ordered, qty_ordered_unit_measure) 
values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    );
 }

sub insert_line_90 {
    my $self = shift;
    my %params = @_;
    
    #the order that the values will be place in the value bind
    @{$self->{ $self->{dataSource}."BindOrder" } } = ('record_id',
                                                      'partnership_id',
                                                      'record_type',
                                                      'number_line_items'
                                                      );
    
    my $sql = qq( 
insert into e856_line_90  
  (record_id, partnership_id, record_type, number_line_items) 
values (?,?,?,?)
    );  
}

#----- do not remove "1" -----#

1;

__END__

