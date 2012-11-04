#!/usr/bin/perl -w
#
# Code shared by the different frontend scripts
#
# A.Gaponenko, 2012
#

package mu2egrid;

use strict;
use Cwd 'abs_path';
use File::Basename;

our $jobsub = 'jobsub';
our @knownOutstage = ('/mu2e/data/outstage', '/grid/data/mu2e/outstage', '/grid/data/marsmu2e/outstage');

our $impldir;
$impldir = abs_path(dirname($0) . '/impl');

#================================================================
sub assert_known_outstage($) {
    my $d = shift;
    foreach my $o (@knownOutstage) {
	$o eq $d and return 1;
    }
    die "The specified outstage \"$d\" is not recognized - is this a typo?  Known location: @knownOutstage\n";
}

#================================================================
sub find_file($) {
    my $fn = shift;
    my $res = abs_path($fn);
    die "Error: file \"$fn\" does not exist\n" unless defined $res;
    return $res;
}

#================================================================
BEGIN {
    use Exporter   ();
    our ($VERSION, @ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS);
    
    # set the version for version checking
    $VERSION = '1.01';

    @ISA         = qw(Exporter);
    @EXPORT      = qw( &find_file &assert_known_outstage $impldir
                      );

    %EXPORT_TAGS = ( all => [qw( &find_file &assert_known_outstage
				 $jobsub $impldir @knownOutstage
                                 )] 
                     );

    # your exported package globals go here,
    # as well as any optionally exported functions
    @EXPORT_OK   = qw( $jobsub $impldir @knownOutstage );
}
our @EXPORT_OK;
use vars @EXPORT_OK;

#================================================================
1;
