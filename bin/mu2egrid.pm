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

our $mu2eDefaultOutstage = $knownOutstage[0];

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
    die "Error: file \"$fn\" does not exist\n"
	unless (defined $res and -e $res);

    return $res;
}

#================================================================
sub validate_prestage_spec($) {
    my $fn = shift;

    die "--prestage-spec is not a regular file: $fn\n" unless (-f $fn or -l $fn);

    if(open(my $fh, $fn)) {
	while(my $line = <$fh>) {
	    chomp($line);
	    $line =~ s/^\s+//;
	    if($line) { # ignore emtpy lines
		$line =~ /^\S+\s+\S+$/ or die "Error: the following --prestage-spec line does not contain two whitespace separated strings:\n$line\n";
		$line =~ m|^\S+\s+[^/\s]+/\S+$| or die "Error: --prestage-spec target filename must contain a slash and not start with a slash.  Bad line:\n$line\n";
	    }
	}
    }
    else {
	die "Error: can not open  file \"$fn\": $!\n";
    }
}

#================================================================
BEGIN {
    use Exporter   ();
    our ($VERSION, @ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS);
    
    # set the version for version checking
    $VERSION = '1.01';

    @ISA         = qw(Exporter);
    @EXPORT      = qw( &find_file &validate_prestage_spec &assert_known_outstage $impldir
                      );

    %EXPORT_TAGS = ( all => [qw( &find_file &validate_prestage_spec &assert_known_outstage
				 $jobsub $impldir @knownOutstage $mu2eDefaultOutstage
                                 )] 
                     );

    # your exported package globals go here,
    # as well as any optionally exported functions
    @EXPORT_OK   = qw( $jobsub $impldir @knownOutstage $mu2eDefaultOutstage );
}
our @EXPORT_OK;
use vars @EXPORT_OK;

#================================================================
1;
