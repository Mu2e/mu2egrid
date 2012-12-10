#!/bin/bash
#
# Grid worker node scrip for mu2e framework jobs.  It is not meant to be
# invoked by users directly.  One should use mu2esub to provide proper
# environment for this script.
#
# Andrei Gaponenko, 2012
#

source "$(dirname $0)/funcs"

#================================================================
createJobFCL() {
    fcl="./mu2e.fcl"    
    /bin/cp "${1:?createJobFCL: arg1 missing}" "$fcl"
    echo "services.scheduler.defaultExceptions : false"             >> "$fcl"
    echo "$fcl"
}

addSeeds() {
    fcl=${1:?addSeeds: arg1 missing}
    echo "services.user.SeedService.policy           :  autoIncrement"                >> "$fcl"
    echo "services.user.SeedService.maxUniqueEngines :  $SeedServiceMaxEngines"       >> "$fcl"
    echo "services.user.SeedService.baseSeed         :  ${2:?addSeeds: arg2 missing}" >> "$fcl"
}

addEventID() {
    fcl=${1:?addEventID: arg1 missing}
    echo "source.firstRun     : ${2:?addEventID: arg2 missing}" >> "$fcl"
    echo "source.firstSubRun  : ${3:?addEventID: arg3 missing}" >> "$fcl"
    echo "source.firstEvent   : ${4:-1}" >> "$fcl"
}

#================================================================
umask 002

export startdir=$(pwd)
export cluster=${CLUSTER:-1}
export process=${PROCESS:-0}
export user=${MU2EGRID_SUBMITTER:?"Error: MU2EGRID_SUBMITTER not set"}
export masterfhicl=${MU2EGRID_MASTERFHICL:?"Error: MU2EGRID_MASTERFHICL not set"}
export userscript=${MU2EGRID_USERSCRIPT:-''}
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
    if source "${MU2EGRID_USERSETUP:?Error: MU2EGRID_USERSETUP: not defined}"; then

        #================================================================
        # There are different typs of jobbs:
        #
        # a) initial G4 generation+simulation.  Does not require input 
        #    data files, but must specify run/subrun number in fcl and seeds.
        #
        # b) A "continuation" G4 job.  Needs input data file and seeds.
        #    Must NOT specify run numbers.
        #
        # c) An analysis job.  Like (b), except it does not require seeds.
        #    There should be no harm in specifying seeds for such jobs, other
        #    then causing unnecessary load of seed svc.  On the other hand
        #    a non-G4 job might use random numbers, so it is safer to always
        #    define random seeds.  Therefore we don't introduce a special 
        #    case for (c) and treat it in this script exactly as (b).
        
        JOBCONFIG=$(createJobFCL "$masterfhicl")
	SEED="${MU2EGRID_BASE_SEED:-$(generateSeed)}"
        addSeeds "$JOBCONFIG" "$SEED"
        
        # mu2e job args: this is the common part of cmdline
        declare -a args=(-c "$JOBCONFIG")

        eventsPrestageSpec=''
        if [ -z "${MU2EGRID_INPUTLIST}" ]; then
            # Case (a): no input file list
            # Define new event IDs
            addEventID "$JOBCONFIG" ${MU2EGRID_RUN_NUMBER:-$cluster} ${process}

	    nevents=${MU2EGRID_EVENTS_PER_JOB:?Error: both MU2EGRID_EVENTS_PER_JOB and MU2EGRID_INPUTLIST not set}
	    # treat --events-per-job=0 as a special case.
	    if [ $nevents -ne 0 ]; then
		args+=(-n ${nevents})
	    fi

        else
            # There are input files specified.
            remoteList=$(createInputFileList ${MU2EGRID_INPUTLIST} ${MU2EGRID_CHUNKSIZE:?"Error: MU2EGRID_CHUNKSIZE not set"} ${process})
	    eventsPrestageSpec=$(createPrestageSpec $remoteList)
	    localList=$(extractLocalList $eventsPrestageSpec)
            args+=(-S "$localList" --nevts -1)
        fi
        
        # Stage input files to the local disk
	stageIn "$eventsPrestageSpec" "$MU2EGRID_PRESTAGE"
	ret=$?

	if [ "$ret" -eq 0 ]; then

            # Run the optional user script
	    if [ -n "$userscript" ]; then
		"$userscript" "$JOBCONFIG" "$process" "$MU2EGRID_NCLUSTERJOBS"
		ret=$?
	    fi

	    # echo "Work dir listing before running the job: ================" >> mu2e.log 2>&1
	    # ls -lR >> mu2e.log 2>&1
	    # echo "================================================================" >> mu2e.log 2>&1

            # Run the Offline job.
	    if [ "$ret" -eq 0 ]; then
		echo "Starting on host $(uname -a) on $(date)" >> mu2e.log 2>&1
		echo "Running the command: mu2e ${args[@]}" >> mu2e.log 2>&1
		echo "mu2egrid random seed $SEED" >> mu2e.log 2>&1
		/usr/bin/time mu2e "${args[@]}" >> mu2e.log 2>&1
		ret=$?
		echo "mu2egrid exit status $ret" >> mu2e.log 2>&1
	    else
		echo "Aborting the job because the user --userscript script failed.  The command line was:" >> mu2e.log 2>&1
		echo ""  >> mu2e.log 2>&1
		echo "$userscript" "$JOBCONFIG" "$process" >> mu2e.log 2>&1
		echo ""  >> mu2e.log 2>&1
		echo "Got exit status: $ret" >> mu2e.log 2>&1
	    fi

	else
	    echo "Aborting the job because pre-staging of input files failed: stageIn '$eventsPrestageSpec' '$MU2EGRID_PRESTAGE'" >> mu2e.log 2>&1
	fi

    else
	echo "Error sourcing setup script ${MU2EGRID_USERSETUP}: status code $?"
	ret=1
    fi
else
    echo "Error sourcing setup script ${MU2EGRID_MU2ESETUP}: status code $?"
    ret=1
fi

#================================================================
# Transfer results (or system info in case of environment problems)

outdir="$(createOutStage ${outstagebase} ${user} ${jobname} ${cluster} ${process})"
transferOutFiles "$outdir" $(filterOutProxy $(selectFiles *) )

exit $ret
