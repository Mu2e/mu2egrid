#!/bin/bash
#
# Grid worker node scrip for mu2e G4beamline jobs.  It is not meant to be
# invoked by users directly.  One should use mu2eg4bl to provide proper
# environment for this script.
#
# Andrei Gaponenko, 2012
#

set -e

source "$(dirname $0)/funcs"

export process=${PROCESS:-0}
export masterin="$CONDOR_DIR_INPUT/${MU2EGRID_MASTERIN:?'Error: MU2EGRID_MASTERIN is not set'}"
if [ -n "${MU2EGRID_PRESTAGE}" ]; then MU2EGRID_PRESTAGE="$CONDOR_DIR_INPUT/$MU2EGRID_PRESTAGE"; fi

#================================================================
# Establish environment.

# UPS setup breaks with "-e", unset it temporary
set +e
if setup G4beamline "${MU2EGRID_G4BLVERSION:?Error: MU2EGRID_G4BLVERSION not set}"; then
    # re-enable exit on error
    set -e

    ret=0

    # Make the input files visible
    if [ -n "$MU2EGRID_TAR" ]; then
        ( cd $CONDOR_DIR_INPUT && tar xf $MU2EGRID_TAR )
    fi
    ln -s $CONDOR_DIR_INPUT/* .

    # g4bl args: this is the common part of cmdline
    declare -a args=("$masterin")

    if [ -z "${MU2EGRID_INPUTLIST}" ]; then
        # The case of no input file list
        # Compute the range of event numbers to generate
        Num_Events=${MU2EGRID_EVENTS_PER_JOB:?Error: both MU2EGRID_EVENTS_PER_JOB and MU2EGRID_INPUTLIST not set}
        First_Event=$(($Num_Events *  $process))
        args+=(First_Event=$First_Event Num_Events=$Num_Events)
    else
            # There are input files specified.
            # Need to append something like
            #
            #     beam ascii filename1
            #     beam ascii filename2
            #     ...
            #     beam ascii filenameN
            #
            # to the .in file.  Need to decide whether the inputs are
            # ROOT or ASCII.  More important, the .in file should be
            # written in a way that supports this.
        echo "mu2eg4bl.sh: input file support is not implemented"
        ret=1
    fi

    # Stage input files to the local disk
    stageIn "$MU2EGRID_PRESTAGE"
    ret=$?

    if [ "$ret" == 0 ]; then
        echo "Starting on host $(uname -a) on $(date)"
        echo "Running the command: g4bl ${args[@]}"
        /usr/bin/time g4bl "${args[@]}"
        ret=$?
        echo "mu2egrid exit status $ret"
    else
        echo "Aborting the job because pre-staging of input files failed: stageIn '$MU2EGRID_PRESTAGE'"
    fi

else
    echo "Error setting up G4beamline ${MU2EGRID_G4BLVERSION:-version not defined}"
fi

#================================================================
exit $ret
