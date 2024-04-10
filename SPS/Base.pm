## Base Class for loads, provides constructor
package MCCS::SPS::Loads::Base;
use strict;
use Carp;

sub new {
    my $class = shift;
    my $util  = shift;
    my $self  = bless( { util => $util }, $class );

    for my $contract ("get_sql", "make_record", "get_filename") {
        if ( !$self->can($contract) ) {
            croak "$class needs to provide an implementation for $contract";
        }
    }

    $self->init();

    $self;
}

sub database { 'rms_r'; }

sub init { }    #stub, do nothing routine

sub finish { my $self = shift; $self->{'util'}->finish(); }  #needs called from subclass

sub DESTROY {
    if ( $_[0] && ref( $_[0] ) ) { $_[0]->finish(); }
}

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::Base - Base interface for all MCCS::SAS extrat plugins

=head1 SYNOPSIS

MCCS::SAS::Loads::Base is an abstract class meant to be inherited by MCCS::SAS::Loads plugins

=head1 DESCRIPTION

The subclass must provide the following methods:

get_sql, make_record, get_filename, site_field, week_limiting

The folowing methods can be overridden:

database, init, finish

The field positions in the get_sql() are expected to match the field positions passed to make_record.

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
