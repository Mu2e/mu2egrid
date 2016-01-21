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
use POSIX qw(ceil);

our $impldir;
$impldir = abs_path(dirname($0) . '/impl');

our @knownOutstage = ('/mu2e/data/outstage',
                      '/mu2e/data2/outstage',
                      '/grid/data/mu2e/outstage',
                      '/grid/data/marsmu2e/outstage',
                      '/pnfs/mu2e/persistent/outstage',
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

sub mu2e_ups_qualifiers($$) {
    my ($mu2esetup, $setup) = @_;
    my $qual = `source $mu2esetup >/dev/null; source $setup >/dev/null; echo \$MU2E_UPS_QUALIFIERS`;
    chomp $qual;
    return $qual;
}

sub default_package_version($$$) {
    my ($mu2esetup, $package, $qualifiers) = @_;
    my $ver = `source $mu2esetup >/dev/null; ups list -K version $package -q '$qualifiers'|head -1`;
    chomp $ver;
    return $ver;
}

our $jobsub = 'jobsub_submit';

our @commonOptList = (

# Mu2e specific things

    'mu2e-setup=s',
    'ifdh-version=s',
    'jobsub-arg=s@',
    'outstage=s',
    "prestage-spec=s",
    'dry-run',
    'verbose',
    'help',

#  Some frequently used jobsub_submit settings

    'group=s',
    'role=s',
    'jobsub-server=s',
    'disk=s',
    'memory=s',
    'expected-lifetime=s',
    'OS=s',
    'resource-provides=s',
    'site=s',
    );

# those that are not defaulted must be tested with exists($opt{'option'}) before accessing their values
our %commonOptDefaultsMu2e = (
    'mu2e-setup' => '/cvmfs/mu2e.opensciencegrid.org/setupmu2e-art.sh',
    'outstage' => $mu2egrid::mu2eDefaultOutstage,
    'dry-run' => 0,
    'verbose' => 0,
    'help' => 0,
    );

# We want all keys to be present in the map, even those that do not
# have defaults.  This is important to not miss any explicitly
# specified options used when building jobsub_submit cmdline.
our %commonOptDefaultsJobsub = (
    default_group_helper(),
    'role' => undef,
    'jobsub-server' => 'https://fifebatch.fnal.gov:8443',
    'disk' => '30GB',
    'memory' => '2048MB',
    'expected-lifetime' => '86400', # 24 hours, but jobsub v1_1_9 wants an int
    'OS' => 'SL6',
    'resource-provides' => 'usage_model=OPPORTUNISTIC,DEDICATED',
    'site' => undef,
    );

sub commonOptDoc1 {
    my %features = @_;

    my $prestageIsSupported = $features{'prestageIsSupported'} // 1;
    my $prestagestr = $prestageIsSupported ? "              [--prestage-spec=<file>] \\\n" : "";

    my $outstageIsSupported = $features{'outstageIsSupported'} // 1;
    my $outstagestr = $outstageIsSupported ? "              [--outstage=<dir>] \\\n" : "";

    return <<EOF
              [--group=<name>] \\
              [--role=<name>] \\
              [--jobsub-server=<URL>] \\
              [--disk=<SizeUnits>] \\
              [--memory=<SizeUnits>] \\
              [--expected-lifetime=<spec>] \\
              [--OS=<comma_separated_list>] \\
              [--resource-provides=<spec>] \\
              [--site=<site1,site2,...>] \\
              [--jobsub-arg=string1] [--jobsub-arg=string2] [...] \\
              [--mu2e-setup=<setupmu2e-art.sh>] \\
              [--ifdh-version=<version>] \\
EOF
.
    $outstagestr
.
    $prestagestr
.    <<EOF
              [--dry-run] \\
              [--verbose] \\
              [--help]
EOF
;
}

sub commonOptDoc2 {
    my %features = @_;

    # legacy default

    my $prestageIsSupported = $features{'prestageIsSupported'} // 1;
    my $outstageIsSupported = $features{'outstageIsSupported'} // 1;

    my $formattedOutstage = join("\n\t\t", ('', @mu2egrid::knownOutstage));
    my $outstageDocString = $outstageIsSupported ? <<EOF

    - Outstage should be one of the following registered locations:
           $formattedOutstage

      by default $mu2eDefaultOutstage is used.
EOF
: '';

    my $res= <<EOF
    - The --group, --role, --jobsub-server, --disk, --memory, --expected-lifetime,
      --OS, --resource-provides, and --site options are passed to jobsub_submit.
      Run \"jobsub_submit -h\" for details. Arbitrary jobsub_submit options
      can be passed using --jobsub-arg.
      The default values are

          --group              $ENV{GROUP} (the GROUP environment variable, if set)
          --role               none
          --jobsub-server      $commonOptDefaultsJobsub{'jobsub-server'}
          --disk               $commonOptDefaultsJobsub{'disk'}
          --memory             $commonOptDefaultsJobsub{'memory'}
          --expected-lifetime  $commonOptDefaultsJobsub{'expected-lifetime'}
          --OS                 $commonOptDefaultsJobsub{'OS'}
          --resource-provides  $commonOptDefaultsJobsub{'resource-provides'}
          --site               none

    --mu2e-setup arg is optional, by default the current official mu2e
      release is used.  The ifdhc package must be available in the
      UPS area set up by the script.

    --ifdh-version=<version> exports the requested IFDH_VERSION to the
      worker node.  It is used by both jobsub package scripts and
      mu2egrid.  If the IFDH_VERSION environment variable is set, it
      will be used.  Otherwise the version seen by the submission
      process as the UPS "current" best match will be used.
EOF
;
    $res .= $outstageDocString;

    if($prestageIsSupported) {
        $res .= <<EOF

    - The --prestage-spec option allows to specify a list of extra
      files that should be prestaged to the worker node.  Each
      line in the specification file has the format:

      /file/name/on/bluarc   relative/file/name/on/worker/node

      that is, source and target file names separated by any amount of
      white space.  The target file name is relative to the working
      directory.  It must contain a slash '/' and must not start with
      a slash. Leading and trailing white spaces are ignored.
EOF
;
    }

    $res .= <<EOF

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
return $res;
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

    die "File list not a regular file: $fn\n" unless (-f $fn or -l $fn);

    my $numlines = 0;
    if(open(my $fh, $fn)) {
        while(my $line = <$fh>) {
            ++$numlines;
            chomp($line);
            die "Error: not an absolute file name: \"$line\" in file $fn\n" unless $line =~ m{^/};
            die "Error: line contains white spaces or other non-printable characters: \"$line\" in file $fn\n" unless $line =~ /^\p{IsGraph}+$/
        }
    }
    else {
        die "Error: can not open  file \"$fn\": $!\n";
    }

    die "Error: empty file $fn\n" unless $numlines > 0;

    return $numlines;
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
sub validate_njobs($$) {
    my ($nfiles,$njobs) = @_;

    die "Invalid njobs = $njobs\n" unless $njobs > 0;

    my $chunkSize = ceil($nfiles/$njobs);
    my $numRequiredJobs = ceil($nfiles/$chunkSize);

    if($numRequiredJobs != $njobs) {
        die "Error: The number of input files to process is $nfiles. ".
            "Splitting them into $njobs jobs will leave some jobs with no inputs. ".
            "Please adjust --njobs (try $numRequiredJobs).\n"
            ;
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
                      @knownOutstage $mu2eDefaultOutstage @commonOptList
                      $jobsub %commonOptDefaultsMu2e %commonOptDefaultsJobsub
                      &commonOptDoc1 &commonOptDoc2
                      &assert_known_outstage &find_file &validate_file_list
                      &validate_prestage_spec
                      );
}
our @EXPORT_OK;
use vars @EXPORT_OK;

#================================================================
1;
