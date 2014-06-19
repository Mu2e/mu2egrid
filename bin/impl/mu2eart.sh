#!/bin/bash
#
# Grid worker node scrip for mu2e framework jobs.  It is not meant to be
# invoked by users directly.  One should use mu2esub to provide proper
# environment for this script.
#
# Andrei Gaponenko, 2012
#

set -e

source "$(dirname $0)/funcs"

#================================================================
createJobFCL() {
    fcl="./mu2e.fcl"    
    /bin/cp "${1:?createJobFCL: arg1 missing}" "$fcl"
    chmod u+w "$fcl"
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

addGeometry() {
    fcl=${1:?addGeometry: arg1 missing}
    geomfile=${2:?addGeometry: no geometry given}
    echo "services.user.GeometryService.inputFile :  \"${geomfile}\"" >>  "$fcl"
}

#================================================================
export cluster=${CLUSTER:-1}
export process=${PROCESS:-0}
export masterfhicl=${MU2EGRID_MASTERFHICL:?"Error: MU2EGRID_MASTERFHICL not set"}
export userscript=${MU2EGRID_USERSCRIPT:-''}

#================================================================
# Establish environment.

# UPS setup breaks with "-e", unset it temporary
set +e
if source "${MU2EGRID_MU2ESETUP:?Error: MU2EGRID_MU2ESETUP: not defined}"; then
    if source "${MU2EGRID_USERSETUP:?Error: MU2EGRID_USERSETUP: not defined}"; then
	# re-enable exit no error
	set -e
    
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

        #================================================================
	# Handle the --fclinput options

        fclinPrestageSpec=''
        if [ ${MU2EGRID_FCLINPUT_NUMENTRIES:-0} -gt 0 ]; then
            fclinPrestageSpec=$(mktemp prestage-fclin.XXXX)
            i=0; while [ $((i+=1)) -le ${MU2EGRID_FCLINPUT_NUMENTRIES:-0} ]; do

                nvn="MU2EGRID_FCLINPUT_${i}_NF"
                numFiles="${!nvn}"
                vvn="MU2EGRID_FCLINPUT_${i}_VAR"
                variable="${!vvn}"
                fvn="MU2EGRID_FCLINPUT_${i}_FILELIST"
                fclinRemoteFiles="${!fvn}"

		# select the given number of files for pre-staging
		tmpremote=$(mktemp prestage-fclin-remote.XXXX)
		if [ $numFiles -gt 0 ]; then
		    pickRandomLines "$fclinRemoteFiles" $numFiles > $tmpremote
		else
		    cat "$fclinRemoteFiles" > $tmpremote
		fi

		# the current chunk of prestage spec
		tmpspec=$(mktemp prestage-fclin-chunk.XXXX)
		createPrestageSpec $tmpremote $tmpspec > /dev/null

		# Format the local list of files and put it into the fcl file variable
		# Should it go to PROLOG, or should it be appended to the file?
		if [[ $variable == '@'* ]]; then
		    # Prepend PROLOG to the file
		    variable="$(echo $variable | sed -e 's/^@//')"
		    tmphead=$(mktemp fclin-prolog.XXXX)
		    echo "BEGIN_PROLOG" > $tmphead
		    echo "$variable : [" >> $tmphead
		    awk 'BEGIN{first=1}; {if(!first) {cm=",";} else {first=0; cm=" ";}; print "    "cm"\""$2"\""}' $tmpspec >> $tmphead
		    echo "]" >> $tmphead
		    echo "END_PROLOG" >> $tmphead
		    cat $tmphead $JOBCONFIG > $JOBCONFIG.$$
		    /bin/mv -f $JOBCONFIG.$$ $JOBCONFIG
		    rm -f $tmphead
		else # Append var assignement at the end of the file
		    echo "$variable : [" >> $JOBCONFIG
		    awk 'BEGIN{first=1}; {if(!first) {cm=",";} else {first=0; cm=" ";}; print "    "cm"\""$2"\""}' $tmpspec >> $JOBCONFIG
		    echo "]" >> $JOBCONFIG
		fi

		# Merge prestage specs, and clean up
		cat $tmpspec >> $fclinPrestageSpec
		rm -f $tmpremote
		rm -f $tmpspec

            done
        fi

        #================================================================
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

        # override histogram filename
        if [ -n "${MU2EGRID_TFILESERVICE}" ]; then
            args+=(-T "${MU2EGRID_TFILESERVICE}")
        fi 

        # override geometry setting
        if [ -n "${MU2EGRID_GEOMETRY}" ]; then
            addGeometry "$JOBCONFIG" "$MU2EGRID_GEOMETRY"
        fi
        
        # Stage input files to the local disk
	stageIn "$eventsPrestageSpec" "$MU2EGRID_PRESTAGE" "$fclinPrestageSpec"
	ret=$?

	if [ "$ret" -eq 0 ]; then

            # Run the optional user script
	    if [ -n "$userscript" ]; then
		"$userscript" "$JOBCONFIG" "$process" "$MU2EGRID_NCLUSTERJOBS"
		ret=$?
	    fi

            # Run the Offline job.
	    if [ "$ret" -eq 0 ]; then
		echo "Starting on host $(uname -a) on $(date)"
		echo "Running the command: mu2e ${args[@]}"
		echo "mu2egrid random seed $SEED"
		/usr/bin/time mu2e "${args[@]}"
		ret=$?
		echo "mu2egrid exit status $ret"
		if [ "$ret" -eq 0 ]; then
                    # clean up
		    rm -f "$eventsPrestageSpec" "$fclinPrestageSpec" "$remoteList" "$localList"
		fi
	    else
		echo "Aborting the job because the user --userscript script failed.  The command line was:"
		echo ""
		echo "$userscript" "$JOBCONFIG" "$process"
		echo ""
		echo "Got exit status: $ret"
	    fi

	else
	    echo "Aborting the job because pre-staging of input files failed: stageIn '$eventsPrestageSpec' '$MU2EGRID_PRESTAGE' '$fclinPrestageSpec'"
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
exit $ret
