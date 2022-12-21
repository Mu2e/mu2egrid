#!/bin/bash
#
# Grid worker node scrip for mu2e G4beamline jobs.  It is not meant to be
# invoked by users directly.  One should use mu2eg4bl to provide proper
# environment for this script.
#
# Andrei Gaponenko, 2012
#

#================================================================
# This function takes an arbitrary number of "prestage specification"
# files, and copies all the remote files to their destinations
# under an I/O throttling protection.
stageIn() {

    ret=0

    declare -a specs
    for f in "$@"; do
        if [ -n "$f" ]; then
            specs=("${specs[@]}" $f)
        fi
    done

    if [ "${#specs[@]}" -gt 0 ]; then

        echo "$(date) # Starting to pre-stage input files"
        type ifdh

        # Merge all the lists into a single file
        # ifdh is picky about white spaces, (redmine #3790)
        # Make sure we have exactly one space
        totalspec=$(mktemp prestage-merged.XXXX)
        awk '{print $1" "$2}' "${specs[@]}" > "$totalspec"

        # ifdh does not create destination directories
        # Do it here
        mkdir -p $(awk '{print $2}' "$totalspec" | sed -e 's|/[^/]*$||' | sort -u)

        awk '{print "Pre-staging: ",$1,"  ==>  ",$2}' "$totalspec"

        tstart=$(date +%s)

        # Get the lock and copy files
        ifdh cp -f "$totalspec"
        ret=$?

        # Do not delete the prestage-merged file - it is useful, e.g. to re-run an exact job.

        t2=$(date +%s)
        echo "$(date) # Total stage-in time: $((t2-tstart)) seconds, status $ret"

    fi

    return $ret
}

#================================================================
# Execution starts here

set -e

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
    if [[ -n "$MU2EGRID_TAR" ]]; then
        # 2022-12-21 jobsub screws up tar files on transer.
        # One has to hide tar file content from jobsub to get it safely transferred.
        # mu2eg4bl uses base64 encoding to achieve this.
        ( cd $CONDOR_DIR_INPUT && base64 -d  $MU2EGRID_TAR | tar xf - )
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
        args+=($MU2EGRID_G4BL_ADD_ARGS)
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
        export G4BEAMLINE="$G4BL_DIR/bin/g4beamline_novis"
        /cvmfs/mu2e.opensciencegrid.org/bin/SLF6/mu2e_time g4bl "${args[@]}"
        ret=$?
        echo "mu2egrid exit status $ret"
        if [ "$ret" -eq 0 ]; then
            # clean up
            rm -f "$MU2EGRID_PRESTAGE"
        fi
    else
        echo "Aborting the job because pre-staging of input files failed: stageIn '$MU2EGRID_PRESTAGE'"
    fi

else
    echo "Error setting up G4beamline ${MU2EGRID_G4BLVERSION:-version not defined}"
fi

#================================================================
exit $ret
