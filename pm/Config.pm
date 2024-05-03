package MCCS::Config;
use strict;
my $pkgname = __PACKAGE__;
use FindBin;
use Data::Dumper;
# These next two variables control the overall operation of this module.
# $cfgstyle determines the type of storage used for everything else.
# Currently only "Xml" and "MySQL" are supported.
# NOTE: These values are case sensitive!

my $cfgstyle = "Xml";
my $basedir = "/usr/local/mccs";
my $cfgdir  = $basedir . "/etc";
my $datadir = $basedir . "/data";
_init();
sub _init {
    eval "require MCCS::Config::$cfgstyle";
    die $@ if $@;
    my $cfgpkg = "Config::$cfgstyle";
    # This call assumes that any sub-package will convert source data to a
    # standard hash format that this module will process further.
    my $apps = $cfgpkg->_init(@_);
    if ( ref($apps) ) {
        foreach my $appname ( keys(%$apps) ) {
            my $VAR1;
            no strict 'refs';
            next if $pkgname->can($appname);
            *{"$pkgname\::$appname"} = sub {
                my $self  = shift;
                my $force = shift;
                return undef unless ref($self);
                return undef unless my $appcfg = $cfgpkg->$appname($force);
                my $VAR1;
                eval( Dumper($appcfg) );
                return ($VAR1);
                }
        }
    }
}
sub new {
    my $proto = shift;
    _init(@_);
    my $self = bless {}, $proto;
    return ($self);
}
sub basedir {
    my $self = shift;
    return ($basedir);
}
 sub cfgdir {
    my $self = shift;
    return ($cfgdir);
}
sub datadir {
    my $self = shift;
    return ($datadir);
}
1;