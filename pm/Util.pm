#Utilities for SAS Planning and Assortment

package MCCS::CMS::Util;
use strict;

use Date::Manip;

use constant NOW => UnixDate( ParseDate('now'), "%q" );

#use MCCS::SAS::InventoryRecord;
#use MCCS::SAS::OnOrderRecord;
#use MCCS::SAS::DateTime;

sub new {
    my $class = shift;
    my ( $database, $filename ) = @_;

    my $filehandle;

    my $is_stdout = 0;

    #open output file
    if ( $filename eq '-' ) {
        $filehandle = \*STDOUT;
        $is_stdout  = 1;
    }
    else {
        die "Can not open '$filename'" unless open( $filehandle, ">$filename" );
    }

    my $db = IBIS::DBI->connect( dbname => $database )
        or die "Can not open Database $database";
    $db->{'AutoCommit'} = undef;

    bless(
        {   db         => $db,
            filehandle => $filehandle,
            filename   => $filename,
            is_stdout  => $is_stdout
        },
        $class
    );
}


sub get_database {
    my $self = shift;
    $self->{'db'};
}

sub finish {
    my $self = shift;
    if ( !$self->{'is_stdout'} ) { close $self->{'filehandle'}; }
}

sub DESTROY {
    my $self = shift;
    if ( ref($self) ) {
        $self->finish();
    }
}

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Util - Utliity methods for SAS data extract

=head1 SYNOPSIS

my $util_obj = MCCS::SAS::Util->new( DBI, filename ); 

=head1 DESCRIPTION

Various utility methods for the MCCS::SAS hierarchy

=head1 SUBROUTINES/METHODS

=over 4

=item set_merch_year()

Set the merch year of interest in this MCCS::SAS

=item set_merch_week()

Set the merch week of interest in this MCCS::SAS

=item get_database()

Get the database of thie MCCS::SAS

=back

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
