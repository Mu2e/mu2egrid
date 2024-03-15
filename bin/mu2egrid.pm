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
use English qw( -no_match_vars ) ; # Avoids regex performance penalty

our $impldir;
$impldir = abs_path(dirname($0) . '/impl');

#================================================================
my %predefinedArgChoices = (
    'none' => ['Do not add any extra command line options.', []],
    'sl7' =>  ["Request SL7 singularity image:\n",
               ['--singularity-image',
                '/cvmfs/singularity.opensciencegrid.org/fermilab/fnal-wn-sl7:latest']
    ],
);

my $predefinedArgChoiceDefault = 'sl7';

sub addPredefinedArgs($$) {
    my ($args, $choice) = @_;
    my $resolved = $predefinedArgChoices{$choice};
    die "Error: unknown value '$choice' for --predefined-args\n"
        unless defined $resolved;

    for my $o (@{$resolved->[1]}) {
        push @$args, $o;
    }
}

#================================================================
sub default_group_helper() {
    my $group = $ENV{'JOBSUB_GROUP'} // $ENV{'GROUP'} // 'mu2e';
    return ('group' => $group);
}

sub default_role_helper() {
    my $role = undef;
    my $user = getpwuid($EFFECTIVE_USER_ID);
    $role = 'Production' if($user eq 'mu2epro');
    return ('role' => $role);
}

sub default_package_version($$$) {
    my ($mu2esetup, $package, $qualifiers) = @_;
    my $ver = `source $mu2esetup >/dev/null; ups list -K version $package -q '$qualifiers'|head -1`;
    # remove the double quotes and spaces printed out by ups
    $ver =~ s/^\s*"//;
    $ver =~ s/"\s*$//;
    return $ver;
}

our $jobsub = 'jobsub_submit';

our @commonOptList = (

# Mu2e specific things
    'predefined-args=s',
    'mu2e-setup=s',
    'ifdh-version=s',
    'ifdh-options=s',
    'jobsub-arg=s@',
    "prestage-spec=s",
    'priority=i',
    'dry-run',
    'verbose',
    'debug',
    'help',

#  Some frequently used jobsub_submit settings

    'group=s',
    'role=s',
    'disk=s',
    'memory=s',
    'expected-lifetime=s',
    'resource-provides=s',
    'site=s',
    );

# those that are not defaulted must be tested with exists($opt{'option'}) before accessing their values
our %commonOptDefaultsMu2e = (
    'predefined-args' => $predefinedArgChoiceDefault,
    'mu2e-setup' => '/cvmfs/mu2e.opensciencegrid.org/setupmu2e-art.sh',
    'priority' => 0,
    'dry-run' => 0,
    'verbose' => 0,
    'debug' => 0,
    'help' => 0,
    );

# We want all keys to be present in the map, even those that do not
# have defaults.  This is important to not miss any explicitly
# specified options used when building jobsub_submit cmdline.
our %commonOptDefaultsJobsub = (
    default_group_helper(),
    default_role_helper(),
    'disk' => '30GB',
    'memory' => '2000MB',
    'expected-lifetime' => '24h',
    'resource-provides' => 'usage_model=OPPORTUNISTIC,DEDICATED',
    'site' => undef,
    );

sub commonOptDoc1 {
    my %features = @_;

    my $prestageIsSupported = $features{'prestageIsSupported'} // 1;
    my $prestagestr = $prestageIsSupported ? "              [--prestage-spec=<file>] \\\n" : "";

    my $predef = join('|', keys %predefinedArgChoices);

    return <<EOF
              [--group=<name>] \\
              [--role=<name>] \\
              [--disk=<SizeUnits>] \\
              [--memory=<SizeUnits>] \\
              [--expected-lifetime=<spec>] \\
              [--resource-provides=<spec>] \\
              [--site=<site1,site2,...>] \\
              [--jobsub-arg=string1] [--jobsub-arg=string2] [...] \\
              [--predefined-args=<$predef>] \\
              [--mu2e-setup=<setupmu2e-art.sh>] \\
              [--ifdh-version=<version>] \\
              [--ifdh-opts=<string>] \\
EOF
.
    $prestagestr
.    <<EOF
              [--priority=<int>] \\
              [--dry-run] \\
              [--verbose] \\
              [--debug] \\
              [--help]
EOF
;
}

sub commonOptDoc2 {
    my %features = @_;

    # legacy default

    my $prestageIsSupported = $features{'prestageIsSupported'} // 1;

    my $keyFieldWidth = 15;
    my $preDoc='';
    foreach my $k (keys %predefinedArgChoices) {
        $preDoc .= ' 'x10 . $k. " "x($keyFieldWidth-length($k)) . $predefinedArgChoices{$k}[0] . "\n";
        $preDoc .= ' 'x14
            . join("\n".' 'x14, @{$predefinedArgChoices{$k}[1]}) . "\n\n";
    }

    my $res= <<EOF
    - The --group, --role, --disk, --memory, --expected-lifetime,
      --resource-provides, and --site options are passed to jobsub_submit.
      Run \"jobsub_submit -h\" for details. Arbitrary jobsub_submit options
      can be passed using --jobsub-arg.
      The default values are

          --group              $commonOptDefaultsJobsub{'group'}
          --role               Production for the mu2epro user, none otherwise
          --disk               $commonOptDefaultsJobsub{'disk'}
          --memory             $commonOptDefaultsJobsub{'memory'}
          --expected-lifetime  $commonOptDefaultsJobsub{'expected-lifetime'}
          --resource-provides  $commonOptDefaultsJobsub{'resource-provides'}
          --site               none

    --predefined-args controls which set of options is added to the
      jobsub command line on top of everything else.
      The choices are:\n\n$preDoc
      The default setting is '$predefinedArgChoiceDefault'.

    --mu2e-setup setting is optional, the default is
      $commonOptDefaultsMu2e{'mu2e-setup'}

    --ifdh-version=<version> exports the requested IFDH_VERSION to the
      worker node.  It is used by both jobsub package scripts and
      mu2egrid.  If the IFDH_VERSION environment variable is set, it
      will be used.  Otherwise the version seen by the submission
      process as the UPS "current" best match will be used.

    --ifdh-options   Is empty by default.  It allows one to pass
      extra options to the ifdh invocations (cp and mv) that deal
      with job output files, like
      --ifdh-options="--force=expftp"

EOF
;
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

    --priority sets the priority of the cluster being submitted with
      respect to your other jobs.  It does not affect the ranking of
      your jobs compared to jobs by other users.  The argument is an
      integer, with both positive and negative values allowed.  The
      default is zero.  Jobs with higher priority will start first
      even if they were submitted later than your other jobs in the
      queue.

    - Use --dry-run to test the submission command without actually
      sending the jobs.  Usually used in conjunction with --verbose.

    - Add --verbose if you want to see the details of what is going on.

    - The --debug options is used during mu2egrid scripts development,
      it tried to avoid things like ifdh retries that are helpful
      in production but slow down the debug cycle.

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
sub find_object($) {
    my $fn = shift;
    my $res = abs_path($fn);
    die "Error: file \"$fn\" does not exist\n"
        unless (defined $res and -e $res);

    return $res;
}

sub find_file($) {
    my $res = &find_object;
    die "Error: \"$res\" is not a file\n" unless -f $res;
    return $res;
}

sub find_directory($) {
    my $res = &find_object;
    die "Error: \"$res\" is not a directory\n" unless -d $res;
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
BEGIN {
    use Exporter   ();
    our ($VERSION, @ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS);

    # set the version for version checking
    $VERSION = '1.01';

    @ISA         = qw(Exporter);
    @EXPORT      = qw();
    ## %EXPORT_TAGS = ( all => [qw( &find_file $impldir )] );

    # your exported package globals go here,
    # as well as any optionally exported functions
    @EXPORT_OK   = qw(
                      $impldir
                      &addPredefinedArgs
                      @commonOptList
                      $jobsub %commonOptDefaultsMu2e %commonOptDefaultsJobsub
                      &commonOptDoc1 &commonOptDoc2
                      &find_file &validate_file_list
                      &validate_prestage_spec
                      );
}
our @EXPORT_OK;
use vars @EXPORT_OK;

#================================================================
1;
