#!/bin/bash
#
# Grid worker node scrip for mu2e MARS jobs.  It is not meant to be
# invoked by users directly.  One should use mu2esub to provide proper
# environment for this script.
#
# Andrei Gaponenko, 2012
#

set -e

source "$(dirname $0)/funcs"

#================================================================
addMARSSeeds() {
    master=${1:?addMARSSeeds: arg1 missing}
    seed=${2:?addMARSSeeds:: arg2 missing}
    sed -e 's/STOP/SEED '$seed'\nSTOP/' $master > MARS.INP
}

#================================================================
export executable=${MU2EGRID_EXECUTABLE:?"Error: MU2EGRID_EXECUTABLE not set"}
export masterinput=${MU2EGRID_MASTERINPUT:?"Error: MU2EGRID_MASTERINPUT not set"}
export topdir=${MU2EGRID_TOPDIR:?"Error: MU2EGRID_TOPDIR not set"}

#================================================================
# Make input files accessible.  These are small files
# (less than 1M), copy them directly without locking.
# Could use symlinks instead, but then ifdh breaks
# on copying them to the outstage, and users want to
# have *INP with the results.

cp $topdir/xsdir .
cp $topdir/*.INP .
cp $topdir/*.f .
/bin/rm -f MARS.INP

# Create job config "MARS.INP" by adding random seeds to the master
SEED="${MU2EGRID_BASE_SEED:-$(generateSeed)}"
addMARSSeeds $masterinput "$SEED"

ret=0
# Stage input files to the local disk
if [ -n "${MU2EGRID_PRESTAGE}" ]; then
    MU2EGRID_PRESTAGE="$CONDOR_DIR_INPUT/$MU2EGRID_PRESTAGE"
    stageIn "$MU2EGRID_PRESTAGE"
    ret=$?
fi

if [ "$ret" == 0 ]; then

    if [ -n "$MU2EGRID_SETUP" ]; then
        echo "Sourcing user setup script $MU2EGRID_SETUP"
        source "$MU2EGRID_SETUP"
    fi

    # Run the job
    echo "Starting on host $(uname -a) on $(date)"
    echo "Running the command: $executable"
    echo "mu2egrid random seed $SEED"
    /usr/bin/time $executable
    ret=$?
    echo "mu2egrid exit status $ret"
else
    echo "Aborting the job because pre-staging of input files failed: stageIn '$MU2EGRID_PRESTAGE'"
    ret=1
fi

exit $ret
