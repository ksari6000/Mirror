package MCCS::SAS::Loads::PRODUCT_BOP;


# SAS Product master plugin master services both full load and weekly changes
use strict;
use base qw(MCCS::SAS::Loads::Base);
use MCCS::SAS::ProductRecord;
use MCCS::SAS::Characteristics;
use MCCS::SAS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    my $db   = $util->get_database();
    my $sth  = $db->prepare(
        "UPDATE sas_product_master SET product_ind = '1' WHERE product_ind IS NULL AND sku_key = ?"
    );
    $self->{'sth_update_product_master'} = $sth;
    $self->{'char_obj'}                  = MCCS::SAS::Characteristics->new($db);
}

# CREATE TABLE sas_product_master (
#  sku_key VARCHAR2(25) NOT NULL PRIMARY KEY,
#  style_id VARCHAR2(14) NOT NULL,
#  color_id VARCHAR2(3) NOT NULL,
#  size_id VARCHAR2(5) NOT NULL,
#  dimension_id VARCHAR2(5) NULL,
#  create_date DATE DEFAULT sysdate NULL,
#  product_ind CHAR(1) NULL,
# style_ind CHAR(1) NULL
# ) TABLESPACE MERCH;

#CREATE INDEX sas_product_master_styleix ON sas_product_master(style_id);

# CREATE TABLE sas_product_master_sc(
# 	sku_key VARCHAR2(25) NOT NULL PRIMARY KEY
# );

# CREATE OR REPLACE VIEW v_sas_barcodes_sku AS
# SELECT sas_utils.sku_key(b.style_id,b.color_id,b.size_id,b.dimension_id) sku_key,
# b.style_id,b.color_id,b.size_id,b.dimension_id
# FROM
# (
#     SELECT distinct style_id, color_id, size_id, dimension_id
#     FROM bar_codes WHERE business_unit_id = 30
# ) b;

# create or replace view style_colors_activity AS
# select 30 business_unit_id, '0' style_id, '0' color_id, 'A' activity_type FROM dual

sub get_sql {
    my $self                  = shift;
    my $new_product           = $self->sql_new_products();
    my $new_style             = $self->sql_style_activity();
    my $style_characteristics = $self->sql_style_characteristics();

    "
SELECT
        base.sku_key,
        s.description,
        base.color_id,
        0 cost,
        0 retail,
        divid,
        lobid,
        deptid,
        deptid || '_' || classid classid,
        deptid || '_' || classid || '_' || nvl(subclassid,'NULL') subclassid,
        s.style_id,
        divdesc,
        lobdesc,
        deptdesc,
        classdesc,
        subclassdesc,
        base.size_id,
        base.dimension_id,
        s.vendor_id,
        sas_utils.get_season_description(s.season_id) season_desc,
        nvl(s.LAST_RECEIPT_DATE,to_date('19000101','YYYYMMDD') ) Last_Receipt_date,	
        get_permanent_retail_price(30,null,s.style_id,base.color_id,base.dimension_id,base.size_id,sysdate,null) retail,
        0 barcode,
        nvl(s.First_RECEIPT_DATE,to_date('19000101','YYYYMMDD') ) First_Receipt_date
FROM 
    (select sm.sku_key, sm.color_id, sm.style_id, sm.size_id, sm.dimension_id from sas_product_master sm
      where (create_date >= trunc(to_date('12-FEB-14','DD-MON-YY')) and Not Exists (Select 1 From Maxdata.Lv10mast\@eric_sasprd Where order_code = sm.sku_key))
      or exists (select 1 from styles sty where sty.style_id = sm.style_id and date_created >= trunc(to_date('12-FEB-14','DD-MON-YY')) )
     union
     select distinct spm.sku_key, spm.color_id, spm.style_id, spm.size_id, spm.dimension_id from sas_product_master spm
      where 
      EXISTS ($style_characteristics)
     
    ) base 
  
    join styles s on (base.style_id = s.style_id)
    JOIN te_item_hier v ON v.styleid = s.style_id
    

WHERE
  s.business_unit_id = 30 and
  Not exists ( select 1 from V_Dept_Class_Subclass V 
                        Join Departments D On (V.Department_Id = D.Department_Id)
                     Where 
                           ((V.Department_Id In (0098)) Or
                           (V.Department_Id = 0853 And V.Class_Id = 5000)) And
                           S.Section_Id = V.Section_Id
                    ) And
  Exists (select 1 from v_sas_barcodes_sku b where b.style_id = base.style_id) 
    
";
}

sub sql_new_products {
    "SELECT 1 
  FROM  sas_product_master 
  WHERE sku_key = b.sku_key 
  AND   product_ind IS NULL";
}

sub sql_style_activity {
    "SELECT 1 
  FROM  style_colors_activity 
  WHERE business_unit_id = 30 
  AND   style_id = b.style_id 
  AND   color_id = b.color_id 
  AND activity_type <> 'D' ";
}

sub sql_style_characteristics {
    "SELECT 1 FROM style_characteristics_activity sca
      WHERE business_unit_id = 30 AND 
            spm.style_id = sca.style_id AND 
            activity_type <> 'D' and 
            creation_date between trunc(to_date('12-FEB-14','DD-MON-YY')) and sysdate";
}

sub make_record {
    my $self = shift;
    my ($upc,          $name,    $color,     $cost,     $price,
        $divid,        $lobid,   $deptid,    $classid,  $subclassid,
        $style,        $divname, $lobname,   $deptname, $classname,
        $subclassname, $size,    $dimension, $vendor,   $season,
        $receipt_date, $perm_price, $barcode, $first_date
    ) = @_;
    $name =~ s/[^[:print:]]//g;
    $name =~ s/\s+$//;

    my %chars = ();
    my %chars_org = ();
    if ( $self->{'char_obj'} ) {
        %chars = $self->{'char_obj'}->get_sas_characteristics($style);

        #---------------------------------------------------------------
        # Since the sequence is not the same as the MERCH Level 7
        # site characteristics, need to remapped here for Product Master
        #---------------------------------------------------------------
        my %tmp;
        %chars_org = %chars;
        $tmp{user_attrib8}  = $chars{user_attrib8} if ( exists $chars{user_attrib8} );     #LifeCycle
        $tmp{user_attrib9}  = $chars{user_attrib1} if ( exists $chars{user_attrib1} );     #distributor

        $tmp{user_attrib29} = $price;    # Current Selling Price
        $tmp{user_attrib30} = "";        # POG Chart 1
        $tmp{user_attrib31} = "";        # POG Chart 2
        $tmp{user_attrib32} = "";        # POG Chart 3

        $tmp{user_attrib33} = $chars{user_attrib33} if ( exists $chars{user_attrib33} );    #Gender
        $tmp{user_attrib34} = $chars{user_attrib34} if ( exists $chars{user_attrib34} );    #Formula
        $tmp{user_attrib35} = $chars{user_attrib35} if ( exists $chars{user_attrib35} );    #Form
        $tmp{user_attrib36} = $chars{user_attrib18} if ( exists $chars{user_attrib18} );    #Sihoute
        $tmp{user_attrib37} = $chars{user_attrib37} if ( exists $chars{user_attrib37} );    #Military issue
        $tmp{user_attrib38} = $chars{user_attrib4}  if ( exists $chars{user_attrib4} );     #Sub Brd 1
        $tmp{user_attrib39} = $chars{user_attrib5}  if ( exists $chars{user_attrib5} );     #Sub Brd 2
        $tmp{user_attrib40} = $chars{user_attrib19} if ( exists $chars{user_attrib19} );    #Processor
        $tmp{user_attrib41} = $chars{user_attrib26} if ( exists $chars{user_attrib26} );    #Fit
        $tmp{user_attrib42} = $chars{user_attrib28} if ( exists $chars{user_attrib28} );    #Texture
        $tmp{user_attrib43} = $chars{user_attrib21} if ( exists $chars{user_attrib21} );    #Corporate Initiatives
        $tmp{user_attrib44} = $chars{user_attrib17} if ( exists $chars{user_attrib17} );    #Hand
        
        %chars = %tmp;
    }

    #use Data::Dumper; print Dumper \%chars;exit;

    my $obj =
        MCCS::SAS::ProductRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   product_id          => $upc,
            'Item Name'         => $name,
            color               => $color,
            Color               => $color,
            item_cost           => $cost,
            current_item_price  => $price,
            'Division ID'       => $divid,
            'LOB ID'            => $lobid,
            'Department ID'     => $deptid,
            'Class ID'          => $classid,
            'Sub Class ID'      => $subclassid,
            'Style ID'          => $style,
            'Division Name'     => $divname,
            'LOB Name'          => $lobname,
            'Department Name'   => $deptname,
            'Class Name'        => $classname,
            'Sub Class Name'    => $subclassname,
            'Style Name'        => $name,
            Dimension           => $dimension,
            Manufacturer        => $vendor,
            'Brand'             => $chars_org{user_attrib12},
            'Holidays/Delivery' => $chars_org{user_attrib13},
            'Fabric'            => $chars_org{user_attrib14},
            'Product Size'      => $chars_org{user_attrib9},
            'Life Style'        => $chars_org{user_attrib7},
            'Collection'        => $chars_org{user_attrib11},
            date_user7          => MCCS::SAS::DateTime->new($receipt_date),
            user_attrib29       => $perm_price,
            'Storage'           => $chars_org{user_attrib20},
            'Density'           => $chars_org{user_attrib3},
            'Key Item'          => $chars_org{user_attrib22},
            'Core Item'         => $chars_org{user_attrib23},
            'Quality'           => $chars_org{user_attrib15},
            'Warranty'          => $chars_org{user_attrib25},
            'Flavor'            => $chars_org{user_attrib6},
            'Proofs Sunscreen'  => $chars_org{user_attrib29},
            'Replenishment'     => $chars_org{user_attrib24},
            'Military Branch'   => $chars_org{user_attrib36},
            user_attrib45       => $barcode,
            %chars,
             user_attrib48      => $first_date
        }
        );

    # HJ $self->product_update($upc);

    $obj->to_string();
}

sub product_update {
    my $self = shift;
    my $upc  = $_[0];
    $self->{'sth_update_product_master'}->execute($upc);
}

sub get_filename {
    'MERCH_10_' . time() . '.txt';
}

sub site_field    { }
sub week_limiting { 0; }
sub databse       {'rms_p'}

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::PRODUCT - MCCS::SAS PRODUCT hiearchy record extract

=head1 SYNOPSIS

MCCS::SAS::PRODUCT->new( MCCS::SAS::Util );

=head1 DESCRIPTION

This plugin extracts the data necessary for the product hiearchy in MCCS::SAS. This is a daily/weekly tracking load.

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
