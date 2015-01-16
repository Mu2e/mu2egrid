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

our $impldir;
$impldir = abs_path(dirname($0) . '/impl');

our @knownOutstage = ('/mu2e/data/outstage',
		      '/mu2e/data2/outstage',
		      '/grid/data/mu2e/outstage',
		      '/grid/data/marsmu2e/outstage',
		      '/pnfs/mu2e/scratch/outstage'
		      );

our $mu2eDefaultOutstage = $knownOutstage[0];

#================================================================
sub default_group_helper() {
    my $group;
    $group = $ENV{'GROUP'} if(defined($ENV{'GROUP'}));
    $group = 'mu2e' unless defined($group);
    return ('group' => $group);
}

sub default_ifdh_helper() {
    my $ver;
    # The same env var is used by the jobsub wrapper.
    $ver = $ENV{'IFDH_VERSION'} if(defined($ENV{'IFDH_VERSION'}));
    $ver = 'v1_7_1' unless defined($ver);
    return ('ifdh-version' => $ver);
}

our $jobsub = 'jobsub_submit';

our @commonOptList = (
		      'group=s',
		      'role=s',
		      'jobsub-server=s',
		      'disk=i',
		      'memory=i',
		      'OS=s',
                      'mu2e-setup=s',
                      'ifdh-version=s',
		      'resource-provides=s',
		      'jobsub-arg=s@',
		      'outstage=s',
		      "prestage-spec=s",
		      'dry-run',
		      'verbose',
		      'help',
		      );

# those that are not defaulted must be tested with exists($opt{'option'}) before accessing their values
our %commonOptDefaults = (
			  default_group_helper(),
			  'jobsub-server' => 'https://fifebatch.fnal.gov:8443',
			  'disk' => '30000', # MB
			  'memory' => '2048', # MB
			  'OS' => 'SL5,SL6',
                          'mu2e-setup' => '/grid/fermiapp/products/mu2e/setupmu2e-art.sh',
                          default_ifdh_helper(),
			  'resource-provides' => 'usage_model=OPPORTUNISTIC,DEDICATED',
			  'outstage' => $mu2egrid::mu2eDefaultOutstage,
			  'dry-run' => 0,
			  'verbose' => 0,
			  'help' => 0,
			  );

sub commonOptDoc1() {
    return <<EOF
	      [--group=<name>] \\
	      [--role=<name>] \\
	      [--jobsub-server=<URL>] \\
	      [--disk=<size_MB>] \\
	      [--memory=<size_MB>] \\
	      [--OS=<comma_separated_list>] \\
	      [--resource-provides=<spec>] \\
	      [--jobsub-arg=string1] [--jobsub-arg=string2] [...] \\
	      [--mu2e-setup=<setupmu2e-art.sh>] \\
	      [--ifdh-version=<version>] \\
	      [--outstage=<dir>] \\
	      [--prestage-spec=<file>] \\
	      [--dry-run] \\
	      [--verbose] \\
	      [--help]
EOF
;
}

sub commonOptDoc2() {
    my $default_ifdh_version = (default_ifdh_helper())[1];
    my $formattedOutstage = join("\n\t\t", ('', @mu2egrid::knownOutstage));
    return <<EOF
    - The --group, --role, --jobsub-server, --disk, --memory, --OS,
      and --resource-provides options are passed to jobsub_submit.
      Arbitrary other jobsub_submit options can be passed using
      --jobsub-arg.  Their default values are

          --group              from the GROUP environment variable
	  --role               none
	  --jobsub-server      $commonOptDefaults{'jobsub-server'}
	  --disk               $commonOptDefaults{'disk'}
	  --memory             $commonOptDefaults{'memory'}
          --OS                 $commonOptDefaults{'OS'}
          --resource-provides  $commonOptDefaults{'resource-provides'}

    --mu2e-setup arg is optional, by default the current official mu2e
      release is used.  The ifdhc package must be available in the
      UPS area set up by the script.

    --ifdh-version=<version> exports the requested IFDH_VERSION to the
      worker node.  It is used by both jobsub wrapper scripts and
      mu2egrid.  The default is $default_ifdh_version.

    - Outstage should be one of the following registered locations:
           $formattedOutstage

      by default $mu2egrid::mu2eDefaultOutstage  is used (except for MARS).

    - The --prestage-spec option allows to specify a list of extra
      files that should be prestaged to the worker node.  Each
      line in the specification file has the format:

      /file/name/on/bluarc   relative/file/name/on/worker/node

      that is, source and target file names separated by any amount of
      white space.  The target file name is relative to the working
      directory.  It must contain a slash '/' and must not start with
      a slash. Leading and trailing white spaces are ignored.

    - Use --dry-run to test the submission command without actually
      sending the jobs.  Usually used in conjunction with --verbose.

    - Add --verbose if you want to see the details of what is going on.

    - The --help option prints this message.

Once grid jobs are submitted the software libraries pointed to by the
setup script, all configuration files, and the input file list, must
be left intact until all the jobs finish.

The square brackets [] above denote optional settings, and
{alternative1|alternative2} constructs denote mutually exclusive
alternatives.  All option names may be abbreviates as long as this is
unambiguous.  (For example, '--verbose' and '--verb' mean the same
thing.)
EOF
;
}

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
sub validate_file_list($) {
    my $fn = shift;

    die "--prestage-spec is not a regular file: $fn\n" unless (-f $fn or -l $fn);

    if(open(my $fh, $fn)) {
	while(my $line = <$fh>) {
	    chop($line);
	    die "Error: not an absolute file name: \"$line\" in file $fn\n" unless $line =~ m{^/};
	    die "Error: line contains white spaces or other non-printable characters: \"$line\" in file $fn\n" unless $line =~ /^\p{IsGraph}+$/
	}
    }
    else {
	die "Error: can not open  file \"$fn\": $!\n";
    }
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
    @EXPORT      = qw();
    ## %EXPORT_TAGS = ( all => [qw( &find_file $impldir @knownOutstage $mu2eDefaultOutstage )] );

    # your exported package globals go here,
    # as well as any optionally exported functions
    @EXPORT_OK   = qw(
		      $impldir
		      @knownOutstage $mu2eDefaultOutstage
		      $jobsub @commonOptList %commonOptDefaults &commonOptDoc1 &commonOptDoc2
		      &assert_known_outstage &find_file &validate_file_list &validate_prestage_spec
		      );
}
our @EXPORT_OK;
use vars @EXPORT_OK;

#================================================================
1;
