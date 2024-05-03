package IBIS::Log::File;

use strict;
use warnings;

use version; our $VERSION = qv('0.0.1');

use IO::File;
use Fcntl ':flock';
use Carp 'croak';
use Readonly;

Readonly::Scalar my $DEFAULT_PERM => oct(644);
Readonly::Scalar my $DEFAULT_MODE => O_WRONLY | O_TRUNC | O_CREAT;
Readonly::Scalar my $DEBUG        => 5;
Readonly::Scalar my $INFO         => 4;
Readonly::Scalar my $SUMMARY      => 4;
Readonly::Scalar my $NOTICE       => 3;
Readonly::Scalar my $WARNING      => 2;
Readonly::Scalar my $CRITICAL     => 1;
Readonly::Scalar my $ERROR        => 1;

use Class::Std;
{
    my ( %append_of, %fh_of, %mode_of, %file_of, %perms_of, %level_of, )
        : ATTRS;

    sub BUILD
    {
        my ( $self, $oid, $arg_ref ) = @_;

        $file_of{$oid} = $arg_ref->{file}
            or croak 'Missing required file parameter in ', ( caller(0) )[3];
        $perms_of{$oid}  = $arg_ref->{perms}  || $DEFAULT_PERM;
        $append_of{$oid} = $arg_ref->{append} || 0;
        $mode_of{$oid}   =
            $arg_ref->{append}
            ? O_WRONLY | O_APPEND | O_CREAT
            : $DEFAULT_MODE;
        $level_of{$oid} = $arg_ref->{level} || $DEBUG;

        $fh_of{$oid}
            = IO::File->new( $file_of{$oid}, $mode_of{$oid}, $perms_of{$oid},
            )
            or croak "Could not create file $arg_ref->{file} in ",
            ( caller(0) )[3], '. Check file ownership and permissions.';

        $fh_of{$oid}->autoflush(1);

        # Force permissions for existing files
        croak "Could not set permissions on $file_of{$oid} in ",
            ( caller(0) )[3]
            unless chmod( $perms_of{$oid}, $file_of{$oid} );

        return;
    }

    sub DEMOLISH
    {
        my ($self) = @_;
        $fh_of{ ident $self}->close() if ( $fh_of{ ident $self} );
        return;
    }

    sub close    ## no critic
    {
        my ($self) = @_;
        return $fh_of{ ident $self}->close();
    }

    sub _log
    {
        my ( $self, $msg ) = @_;

        if ( $msg =~ /\[debug|warning\]/msx ) { print STDERR $msg; }

        flock( $fh_of{ ident $self}, LOCK_EX );
        $fh_of{ ident $self}->print($msg);
        flock( $fh_of{ ident $self}, LOCK_UN );
        return 1;
    }

    sub _msg
    {
        my ( $priority, $msg ) = @_;
        my $time = localtime;
      #  my ( $pack, $filename, $line ) = caller(1);
        # This reports the line of code the malformed message was declared
      #  $msg = join q{ }, " at $filename line $line\n"
      #      unless $msg =~ /\n\z/msx;

        chomp $msg;

        if ( $priority eq 'summary') {
            return "$msg\n";
        } else {
            return "$time [$priority] $msg\n";
        }
    }

    # Backward compatibility section
    sub log_error
    {
        my ($self) = shift;
        $self->error(@_);
    }

    sub log_debug
    {
        my ($self) = shift;
        $self->debug(@_);
    }

    sub log_summary
    {
        my ($self) = shift;
        $self->summary(@_);
    }

    sub log_info
    {
        my ($self) = shift;
        $self->info(@_);
    }

    sub log_notice
    {
        my ($self) = shift;
        $self->notice(@_);
    }

    sub log_warn
    {
        my ($self) = shift;
        $self->warn(@_);
    }

    sub log_die
    {
        my ($self) = shift;
        $self->die(@_);
    }
    # END backward compatibility section

    sub debug
    {
        my ( $self, $string ) = @_;

        return unless $DEBUG <= $level_of{ ident $self};
        return _log( $self, _msg( 'debug', $string ) );
    }

    sub error
    {
        my ( $self, $string ) = @_;
        warn $string;
        return _log( $self, _msg( 'error', $string ) );
    }

    sub info
    {
        my ( $self, $string ) = @_;

        return unless $INFO <= $level_of{ ident $self};
        return _log( $self, _msg( 'info', $string ) );
    }

    sub summary
    {
        my ( $self, $string ) = @_;

        return unless $SUMMARY <= $level_of{ ident $self};
        return _log( $self, _msg( 'summary', $string ) );
    }

    sub notice
    {
        my ( $self, $string ) = @_;

        return unless $NOTICE <= $level_of{ ident $self};
        return _log( $self, _msg( 'notice', $string ) );
    }

    sub warn
    {
        my ( $self, $string ) = @_;

        return unless $WARNING <= $level_of{ ident $self};
        warn $string;
        return _log( $self, _msg( 'warning', $string ) );
    }

    sub die
    {
        my ( $self, $string ) = @_;
        _log( $self, _msg( 'critical', $string ) );
        #die $string;
        croak $string;
        return;
    }

    sub level
    {
        my ( $self, $priority ) = @_;
        $level_of{ ident $self} = $priority;
        return $level_of{ ident $self};
    }
}

1;

__END__

=pod

=head1 NAME

IBIS::Log::File - Object Oriented interface for application logging.

=head1 VERSION

This documentation refers to IBIS::Log::File version 0.0.1.

=head1 SYNOPSIS

    use IBIS::Log::File;

    my $log = IBIS::Log::File->new( {file => $filename} );

    $log->debug("This is a debug message\n");
    $log->info("This is an informational message\n");
    $log->summary("This is an summary message, not including date\n");
    $log->notice("This is a notice message\n");
    $log->warn("This is a warining message\n");
    $log->die("This is a message from the dead\n");

    # Full debug logging
    $log->level(5);

    # Log only error and critical messages
    $log->level(1);

=head1 DESCRIPTION

C<IBIS::Log::File> was inspired by Lincoln Stein's LogFile package in Network Programming with Perl. This module provides an easy to use log file interface with output similar to syslog. This module exists to standardize logging to files under IBIS.

Logfiles should be immutable so this object creates 'write only' files. This means there are no facilities for seeking, modifying or otherwise editing files, only output.

Logging defaults to level 4 or full logging. The log level may be reduced with the level method or set to a lower value in the constructor. Level 5 is the most verbose, level 1 is the least verbose logging only errors and critical messages.

This object's destructor takes care of closing the file handle.

=head1 SUBROUTINES/METHODS

=over 4

=item new()

The constructor takes up to four parameters 'file', 'perms', 'append', and 'priority'. The only required parameter is 'file'.

The 'file' parameter sets the name of the file for logging output. Specify the fully qualified path name here.

For example:

    my $log = IBIS::LogFile->new( {file => '/var/log/mylogfile.log'} );

The 'perms' parameter is used to set permissions on the output file. File permissions default to 0644 (i.e. owner read/write, all others read only). You may explicitly set the permissions using the 'perms' parameter. You cannot use quotes for the file permissions or they will not be applied.

For example:

    # Works
    my $log = IBIS::LogFile->new( {file => $filename, $perm => 0644} );

    # Doesn't work
    my $log = IBIS::LogFile->new( {file => $filename, $perm => '0644'} );

File mode is set to write only, truncate, create by default. If you wish to append to a log file, set append to a true value.

For example:

    my $log = IBIS::LogFile->new( {file => $filename, append => 1} );

The priority level for messages may be set in the constructor or after the object is created with the priority method.

For example:

    # Log messages only messages with a level of ERROR or CRITICAL
    my $log = IBIS::LogFile->new( {file => $filename, level => 1} );

=item close()

Close the filehandle on the log file.

=item debug()

Logs messages with a priority of DEBUG. Debug messages are printed to the screen in addition to being written to the log file.

=item error()

Log messages with a priority of ERROR.

=item summary()

Logs messages with a priority of SUMMARY. This is the standard level of logging used for summary messages, no date will be printed.

=item info()

Logs messages with a priority of INFO. This is the standard level of logging used for informational messages.

=item notice()
 
Logs messages with a priority of NOTICE.

=item warn()

Logs messages with a priority of WARNING.

=item die()

Logs messages with a priority of CRITICAL and dies with the message string.

=item level()

Sets the verbosity level for message logging. Default is DEBUG or 5, log everything. The message level constants are not exported. You have to specify the numeric value for the priority level. Only messages less than or equal to the set level are logged. Level values are:

    DEBUG     => 5      # Log everything
    INFO      => 4      # Standard messages
    NOTICE    => 3
    WARNING   => 2
    CRITICAL  => 1
    ERROR     => 1      # Errors are always logged

    Example:

    $log->level(2);     # Log warning, critical and error

=back

=head1 INTERNAL METHODS

=over

=item BUILD

Class::Std initializer.

=item DEMOLISH

Class::Std destructor.

=item _msg()

Concatenates the log entry. Adds time stamp, log level, and message string. Calls _log() for writing.

=item _log()

Receives message string, locks log file for writing, writes message and unlocks file. This method also checks for debug and warning messages and prints them to STDERR.

=back

=head1 DIAGNOSTICS

=over

=item Missing required file parameter in IBIS::Log::File::new

You didn't day my $csv = IBIS::Log::File::->new( { file => './logfile.log' } )

=item Could not create file %s in IBIS::Log::File::new

There was an error creating the file. Check file permissions.

=item Could not set permissions on %s in IBIS::Log::File::new.

There was a problem setting the permissions on the file.

=back

=head1 CONFIGURATION AND ENVIRONMENT

None.

=head1 DEPENDENCIES

=over

=item *

L<IO::File>

=item *

L<Fcntl>

=item *

L<Class::Std>

=item *

L<Carp>

=back

=head1 INCOMPATIBILITIES

None.

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.
Please report problems to Trevor S. Cornpropst L<tcornpropst@acm.org|mailto:tcornpropst@acm.org>.
Patches are welcome.

=head1 AUTHOR

Trevor S. Cornpropst L<tcornpropst@acm.org|mailto:tcornpropst@acm.org>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2006 Trevor S. Cornpropst. All rights reserved.

This module is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut

