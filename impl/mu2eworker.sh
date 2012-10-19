#!/bin/bash
#
# Grid worker node scrip for mu2e framework jobs.  It is not meant to be
# invoked by users directly.  One should use mu2esub to provide proper
# environment for this script.
#
# Andrei Gaponenko, 2012
#

# static config:
SeedServiceMaxEngines=20

#================================================================
createOutStage() {
    # Copy arguments into meaningful names.
    outstagebase=${1:?createOutStage: outstagebase missing}
    user=${2:?createOutStage: user missing}
    jobname=${3:?createOutStage: jobname missing}
    cluster=${4:?createOutStage: cluster missing}
    process=${5:?createOutStage: process missing}

    outtop="${outstagebase}/$user/${jobname}.${cluster}"
    outstage="${outtop}/$(printf '%05d' $process)"

    mode=0775

    mkdir -p --mode "$mode" "${outtop}" \
	&& mkdir --mode "$mode" "${outstage}" \
	&& echo "${outstage}"
}
#================================================================
generateSeed() {
    # art's RandomNumberGenerator_service restrict seeds to
    # not exceed 900000000.   Not clear if zero seed is OK
    # so we'll use a non-negative number up to the max.
    # Should leave space for SeedService to increment, thus -20.
    seed=0
    maxseed=$((900000001 - $SeedServiceMaxEngines))
    while [ "$seed" -le 0 ]; do 
	seed=$(( $(od --format u4 --read-bytes 4 /dev/urandom | head -1| awk '{print $2}') % maxseed ))
    done
    echo $seed
}

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
createInputFileList() {
    masterlist=${1:?createInputFileList: masterlist arg missing}
    chunksize=${2:?createInputFileList: chunksize arg missing}
    process=${3:?createInputFileList: process arg missing}

    firstline=$((1 + $chunksize * $process))
    
    mylist="mu2eInputFiles.txt"
    tail --lines=+"$firstline" "$masterlist" | head --lines="$chunksize" > "$mylist"
    echo "$mylist"
}

#================================================================
printinfo() {
    echo Starting on host `uname -a` on `date`
    echo running as user `whoami`
    echo "current work dir is $(/bin/pwd)"
    echo OS version `cat /etc/redhat-release`
    echo "job arguments: $@"
    echo "The environment is:"
    /usr/bin/printenv
    echo "================================================================"
    echo "Visible disk space:"
    df -P
    echo "================================================================"
    echo "TMPDIR: ls -al"
    ls -al "$TMPDIR"
    echo "TMPDIR: df -h"
    df -h "$TMPDIR"
}

#================================================================
umask 002

export startdir=$(pwd)
export cluster=${CLUSTER:-1}
export process=${PROCESS:-0}
export user=${MU2EGRID_SUBMITTER:?"Error: MU2EGRID_SUBMITTER not set"}
export masterfhicl=${MU2EGRID_MASTERFHICL:?"Error: MU2EGRID_MASTERFHICL not set"}
export fcledit=${MU2EGRID_FCLEDIT:-''}
export jobname=${MU2EGRID_JOBNAME:?"Error: MU2EGRID_JOBNAME not set"}

#outstagebase="/mu2e/data/outstage"
#outstagebase="/grid/data/mu2e/outstage"
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
        addSeeds "$JOBCONFIG" "${MU2EGRID_BASE_SEED:-$(generateSeed)}"
        
        # mu2e job args: this is the common part of cmdline
        declare -a args=(-c "$JOBCONFIG")
        
        if [ -z "${MU2EGRID_INPUTLIST}" ]; then
            # Case (a): no input file list
            # Define new event IDs
            addEventID "$JOBCONFIG" ${MU2EGRID_RUN_NUMBER:-$cluster} ${process}
            args+=(-n ${MU2EGRID_EVENTS_PER_JOB:?Error: both MU2EGRID_EVENTS_PER_JOB and MU2EGRID_INPUTLIST not set})
        else
            # There are input files specified.
            mylist=$(createInputFileList ${MU2EGRID_INPUTLIST} ${MU2EGRID_CHUNKSIZE:?"Error: MU2EGRID_CHUNKSIZE not set"} ${process})
            args+=(-S "$mylist")
        fi
        
        # NB: can stage large input files here to local disk
        # Is this useful/needed?

        # Run the optional fcledit user script
	ret=0
	if [ -n "$fcledit" ]; then
	    "$fcledit" "$JOBCONFIG" "$process"
	    ret=$?
	fi

        # Run the Offline job.
	if [ "$ret" -eq 0 ]; then
	    echo "Starting on host $(uname -a) on $(date)" >> mu2e.log 2>&1
	    echo "Running the command: mu2e ${args[@]}" >> mu2elog 2>&1
	    /usr/bin/time mu2e "${args[@]}" >> mu2e.log 2>&1
	    ret=$?
	else
	    echo "Aborting the job because the user --fcledit script failed.  The command line was:" >> testlog.log 2>&1
	    echo ""  >> testlog.log 2>&1
	    echo "$fcledit" "$JOBCONFIG" "$process" >> testlog.log 2>&1
	    echo ""  >> testlog.log 2>&1
	    echo "Got exit status: $ret" >> testlog.log 2>&1
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

OUTDIR="$(createOutStage ${outstagebase} ${user} ${jobname} ${cluster} ${process})"

/grid/fermiapp/minos/scripts/lock 
for f in *; do
    case "$f" in
	*.proxy)
            # Don't expose security sensitive info
	    echo Skipping proxy file $f;;
	*) 
	    CMD="${GLOBUS_LOCATION}/bin/globus-url-copy -dbg -vb  file://${WORKDIR}/$f gsiftp://if-gridftp-mu2e.fnal.gov//${OUTDIR}/"
	    echo "about to do gridftp, the command is $CMD"
	    $CMD ;;
    esac
done
/grid/fermiapp/minos/scripts/lock free

exit $ret
