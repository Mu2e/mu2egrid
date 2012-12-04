#!/bin/bash
#
# Grid worker node scrip for mu2e G4beamline jobs.  It is not meant to be
# invoked by users directly.  One should use mu2eg4bl to provide proper
# environment for this script.
#
# Andrei Gaponenko, 2012
#

source "$(dirname $0)/funcs"

#================================================================
umask 002

export startdir=$(pwd)
export cluster=${CLUSTER:-1}
export process=${PROCESS:-0}
export user=${MU2EGRID_SUBMITTER:?"Error: MU2EGRID_SUBMITTER not set"}
export masterin=${MU2EGRID_MASTERIN:?"Error: MU2EGRID_MASTERIN not set"}
export jobname=${MU2EGRID_JOBNAME:?"Error: MU2EGRID_JOBNAME not set"}
export outstagebase=${MU2EGRID_OUTSTAGE:?"Error: MU2EGRID_OUTSTAGE not set"}

#================================================================
# TMPDIR is defined and created by Condor.
WORKDIR="$TMPDIR"
{ [[ -n "$WORKDIR" ]] && mkdir -p "$WORKDIR"; } || \
    { echo "ERROR: unable to create temporary directory!" 1>&2; exit 1; }
# Condor will get rid of any files we leave anyway, but we
# can also clean up our own files
trap "[[ -n \"$WORKDIR\" ]] && { cd /; rm -rf \"$WORKDIR\"; }" 0
#
#
cd $WORKDIR

printinfo > sysinfo.log 2>&1 

#================================================================
# Establish environment.

if source "${MU2EGRID_MU2ESETUP:?Error: MU2EGRID_MU2ESETUP: not defined}"; then
    if setup G4beamline "${MU2EGRID_G4BLVERSION:?Error: MU2EGRID_G4BLVERSION not set}"; then

	ret=0
	
        # Make the input files visible 
	if [ -n "$MU2EGRID_TAR" ]; then
	    ( cd $CONDOR_DIR_INPUT && tar xf $MU2EGRID_TAR )
	fi
	ln -s $CONDOR_DIR_INPUT/* .

        # g4bl args: this is the common part of cmdline
	declare -a args=("$masterin")
	
	if [ -z "${MU2EGRID_INPUTLIST}" ]; then
            # Case no input file list
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
	    echo "mu2eg4bl.sh: input file support is not implemented" >> mu2e.log 2>&1
	    ret=1
	fi
	
        # NB: can stage large input files here to local disk
        # Is this useful/needed?

	if [ "$ret" == 0 ]; then
	    echo "Starting on host $(uname -a) on $(date)" >> mu2e.log 2>&1
	    echo "Running the command: g4bl ${args[@]}" >> mu2e.log 2>&1
	    /usr/bin/time g4bl "${args[@]}" >> mu2e.log 2>&1
	    ret=$?
	    echo "mu2egrid exit status $ret" >> mu2e.log 2>&1
	fi

    else
	echo "Error setting up G4beamline ${MU2EGRID_G4BLVERSION:-version not defined}"
    fi
else
    echo "Error sourcing setup script ${MU2EGRID_MU2ESETUP}: status code $?"
    ret=1
fi

#================================================================
# Transfer results (or system info in case of environment problems)

outdir="$(createOutStage ${outstagebase} ${user} ${jobname} ${cluster} ${process})"
# Ignore symlinks to the input files
transferOutFiles "$outdir" $(filterOutProxy $(selectFiles *) )

exit $ret
