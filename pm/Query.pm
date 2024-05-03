#######################################
#This is for WMS and using the same queries in multiple spots
#Ibis, BIN perl program, etc
# 
#######################################

package IBIS::EmpowerIT::Query;

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
# Hey what I am calling in the BASE has the missing link that
# makes this all work so easily.... Code once, Use Anywhere, 
#--------------------------------------------------------------------------

sub get_record {
    my $self = shift;
    my %params = @_;
        
    for my $key (keys %params) {
        $self->{$key} = $params{$key} 
    }
    
    my $sql;      
    $sql = $self->get_curr_merch_week() if ('getCurrMerchWeek' eq $self->{dataSource}) ;
    $sql = $self->get_last_closed_week() if ('getLastClosedWeek' eq $self->{dataSource}) ;
    $sql = $self->get_sales_data() if ('getSalesData' eq $self->{dataSource}) ;
    $sql = $self->get_sales_data_merch() if ('getSalesDataMerch' eq $self->{dataSource}) ;
    $sql = $self->get_incomplete_sale() if ('boolIncompleteSale' eq $self->{dataSource}) ;
    
    my $results;
    $results = $self->single_row_sql( $sql, \%params) if ('getCurrMerchWeek' eq $self->{dataSource}) ;  
    $results = $self->single_row_sql( $sql, \%params) if ('getLastClosedWeek' eq $self->{dataSource}) ;  
    $results = $self->multi_row_sql( $sql, \%params) if ('getSalesData' eq $self->{dataSource}) ;  
    $results = $self->multi_row_sql( $sql, \%params) if ('getSalesDataMerch' eq $self->{dataSource}) ;  
    $results = $self->single_row_sql( $sql, \%params) if ('boolIncompleteSale' eq $self->{dataSource}) ;  
    
    return $results;   
      
}

#sub set_record {}


#--------------------------------------------------------------------------#
#     Select Query Section
#--------------------------------------------------------------------------#
sub get_curr_merch_week {
    my $self = shift;
    my %params = %{$self};
      
    my $sql = qq(

select 
'current' week_type,
m.merchandising_year, m.merchandising_week, m.merchandising_period, 
 to_char(date_closed, 'mm/dd/yyyy' ) date_closed, 
 to_char((week_ending_date - 6), 'mm/dd/yyyy') week_start_date,
 to_char(week_ending_date, 'mm/dd/yyyy' ) week_ending_date
from MERCHANDISING_CALENDARS M 
where business_unit_id = '30' and
sysdate between WEEK_ENDING_DATE - 6 and WEEK_ENDING_DATE

    );

return $sql;

 }
 
 sub get_last_closed_week {
    my $self = shift;
    my %params = %{$self};
         
    my $sql = qq( 

select 
'previous' week_type,
m.merchandising_year, m.merchandising_week, m.merchandising_period, 
to_char(date_closed, 'mm/dd/yyyy' ) date_closed, 
to_char((week_ending_date - 6), 'mm/dd/yyyy') week_start_date,
to_char(week_ending_date, 'mm/dd/yyyy' ) week_ending_date
from 
 (select * from merchandising_calendars 
  where date_closed is not null 
  order by date_closed desc) m
where rownum = 1

    );

return $sql;

 }
 
  sub get_incomplete_sale {
    my $self = shift;
    my %params = %{$self};
    
    my $sql = qq( 
select count(*) incomplete_sales from 
(select * from sales s
 where business_unit_id = '30' and
       sale_date >= to_date('$params{week_start_date}','mm/dd/yyyy') and
       exists (select 1 from sites where s.site_id = site_id) 
       and update_status <> 'COMP'
)
where sale_date <= to_date('$params{week_ending_date}','mm/dd/yyyy')

    );

return $sql;

 }
 
  sub get_sales_data {
    my $self = shift;
    my %params = @_;
         
    my $sql = qq( 

SELECT
(select 1 from dual where  REGEXP_LIKE(s.description, '\\,|\\"') ) punct_flg,
 m.merchandising_year,
 m.merchandising_period,
 m.merchandising_week,
 to_char(m.week_ending_date, 'yyyymmdd') week_ending_date,
 SD.SITE_ID,
 ss.name,
 sd.bar_code_id,
 S.DESCRIPTION,
 v.department_id,
 v.dept_name,
 v.class_id,
 v.class_descr,
 v.sub_class_id,
 v.sub_class_descr,
 SUM(SD.QTy) qty,
 sum(sd.extension_amount) extension_amount

FROM 
 SALE_DETAILS   SD 
 
 join STYLES S on (SD.BUSINESS_UNIT_ID = S.BUSINESS_UNIT_ID AND
                   SD.STYLE_ID = S.STYLE_ID)
 
 join BAR_CODES B on (SD.BUSINESS_UNIT_ID = B.BUSINESS_UNIT_ID AND
                      SD.bar_code_sub_type = B.sub_type AND
                      SD.BAR_CODE_ID = B.BAR_CODE_ID)
 
 join QSENSE.V_DEPT_CLASS_SUBCLASS2 V on(s.BUSINESS_UNIT_ID = V.BUSINESS_UNIT_ID AND
                                         S.SECTION_ID = V.SECTION_ID)
 
 /*MERCHANDINSG CAL AND LAST CLOSE WEEK */                                
 join (select merchandising_period, merchandising_year, merchandising_week,
       (week_ending_date - 6) week_start_date, week_ending_date from 
        (select * from merchandising_calendars where date_closed is not null 
         order by date_closed desc)
      where rownum = 1) m on (1 = 1)
 
 join sites ss on (sd.business_unit_id = ss.business_unit_id and
                   sd.site_id = ss.site_id)
                
WHERE 
 SD.BUSINESS_UNIT_ID = 30 AND
 sd.sale_date > week_start_date - 1 and 
 SD.SALE_DATE BETWEEN m.week_start_date and week_ending_date and
 sd.sub_type = 'ITEM' AND
 v.inventory = 'Y'
GROUP BY
 m.merchandising_year,  m.merchandising_period,
 SD.SITE_ID,  ss.name,  sd.bar_code_id,  S.DESCRIPTION,  v.department_id,
 v.dept_name,  v.class_id,  v.class_descr,  v.sub_class_id,  v.sub_class_descr,
 M.MERCHANDISING_WEEK, M.WEEK_ENDING_DATE

);

return $sql;

 }

sub get_sales_data_merch {
    my $self = shift;
    my %params = %{$self};
         
    my $sql = qq( 

SELECT
(select 1 from dual where  REGEXP_LIKE(s.description, '\\,|\\"') ) punct_flg,
 m.merchandising_year,
 m.merchandising_period,
 m.merchandising_week,
 to_char(m.week_ending_date, 'yyyymmdd') week_ending_date,
 SD.SITE_ID,
 ss.name,
 sd.bar_code_id,
 S.DESCRIPTION,
 v.department_id,
 v.dept_name,
 v.class_id,
 v.class_descr,
 v.sub_class_id,
 v.sub_class_descr,
 SUM(SD.QTy) qty,
 sum(sd.extension_amount) extension_amount

FROM 
 SALE_DETAILS   SD 
 
 join STYLES S on (SD.BUSINESS_UNIT_ID = S.BUSINESS_UNIT_ID AND
                   SD.STYLE_ID = S.STYLE_ID)
 
 join BAR_CODES B on (SD.BUSINESS_UNIT_ID = B.BUSINESS_UNIT_ID AND
                      SD.bar_code_sub_type = B.sub_type AND
                      SD.BAR_CODE_ID = B.BAR_CODE_ID)
 
 join QSENSE.V_DEPT_CLASS_SUBCLASS2 V on(s.BUSINESS_UNIT_ID = V.BUSINESS_UNIT_ID AND
                                         S.SECTION_ID = V.SECTION_ID)
 
 /*MERCHANDINSG CAL AND LAST CLOSE WEEK */                                
 join (select merchandising_period, merchandising_year, merchandising_week,
       (week_ending_date - 6) week_start_date, week_ending_date 
      from merchandising_calendars
      where merchandising_year = '$params{merch_year}'
        and merchandising_week = '$params{merch_week}') m on (1 = 1)
 
 join sites ss on (sd.business_unit_id = ss.business_unit_id and
                   sd.site_id = ss.site_id)
                
WHERE 
 SD.BUSINESS_UNIT_ID = 30 AND
 sd.sale_date > week_start_date - 1 and 
 SD.SALE_DATE BETWEEN m.week_start_date and week_ending_date and
 sd.sub_type = 'ITEM' AND
 v.inventory = 'Y' 
 GROUP BY
 m.merchandising_year,  m.merchandising_period,
 SD.SITE_ID,  ss.name,  sd.bar_code_id,  S.DESCRIPTION,  v.department_id,
 v.dept_name,  v.class_id,  v.class_descr,  v.sub_class_id,  v.sub_class_descr,
 M.MERCHANDISING_WEEK, M.WEEK_ENDING_DATE

);

return $sql;

 }
#--------------------------------------------------------------------------#
#     Insert/Update Query Section
#--------------------------------------------------------------------------#


#Nothing at this time


#-----------------------------------------------------
# Calling Store procedure with values being return
#-----------------------------------------------------
# sub example_sp {
#   #calls Database SP
#    my $self = shift;
#    my %params = @_;
#    
#    #has to be same as params passing from calling program
#    @{$self->{ $self->{dataSource}."bind_in_out" } } = ('site_id:in',
#                                                        'merch_year:in',
#                                                        'mc_config_id:in',
#                                                        'eff_date:in',
#                                                        'process:in',
#                                                        'modified_by:in',
#                                                        'mc_sqft_hdr_id:out',
#                                                        'found:out'
#                                                       );
#    
#    my $sql = qq( 
#      Begin create_example_sp(:site_id, :merch_year, :mc_config_id, :eff_date, :process, :modified_by, :mc_sqft_hdr_id, :found ); End;  
#    );
#
#}
#----------------------------------------------------- 
# In your calling program make sure you init then use
# below example.  All your return values will be in the corresponding
# hash ref with the name you gave in "bind_in_out" 
#-----------------------------------------------------
# $exmpRtn = $dataAccess->set_record( dataSource    => 'example_sp', 
#                                         merch_year   => $ARGS{merch_year},
#                                         site_id      => $ARGS{site_id},
#                                         eff_date     => $ARGS{eff_date},
#                                         modified_by  => $user->username(),
#                                         process      => 'exists',
#                                         mc_config_id => $ARGS{mc_config_id_fk}, );
#
# $exmpRtn->{found} would be in the ref due to it being an 'out' value of the Oracle SP





#----- do not remove "1" -----#

1;

__END__

