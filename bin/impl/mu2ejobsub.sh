#!/bin/bash
#
# Grid worker node scrip for mu2e framework jobs.  It is not meant to
# be invoked by users directly.  One should use the mu2ejobsub user
# interface program to prepare the environment and submit this script.
#
# Andrei Gaponenko, 2014, 2015, 2024
#

error_delay="${MU2EGRID_ERRORDELAY:?Error: MU2EGRID_ERRORDELAY is not defined}"
errfile=$TMPDIR/mu2ejobsub_errmsg.$$

#================================================================
printinfo() {
    echo ${1:-Starting} on host `uname -a` on `date` -- $(date +%s)
    echo \$0 is $0
    echo running as user `id`
    echo "current work dir is $(/bin/pwd)"
    echo OS version `cat /etc/redhat-release`
    echo "#================================================================"
    echo "Visible disk space:"
    df -P
    echo "#================================================================"
    echo "TMPDIR: ls -alR"
    ls -alR "$TMPDIR"
    echo "TMPDIR: df -h"
    df -h "$TMPDIR"
    echo "#================================================================"
    echo "cat /proc/cpuinfo"
    cat /proc/cpuinfo
    if [[ $MU2EGRID_DEBUG > 0 ]]; then
        echo "#================================================================"
        echo "# printenv "
        /usr/bin/printenv
        echo "#================================================================"
        echo "# ls -l \$CONDOR_DIR_INPUT/"
        ls -l $CONDOR_DIR_INPUT/
        echo "#================================================================"
    fi
    echo "#================================================================"
}

#================================================================
addManifest() {
    manifest=${1:?addManifest: logFileName is missing}
    shift
    echo "mu2ejobsub diskUse = $(du -ks)" >> $manifest
    echo '#================================================================' >> $manifest
    echo '# mu2egrid manifest' >> $manifest
    ls -al |awk '{print "# "$0}' >> $manifest
    echo '#----------------------------------------------------------------' >> $manifest
    echo '# algorithm: sha256sum' >> $manifest
    sha256sum "$@" >> $manifest
    sc="$(sha256sum < $manifest)"
    echo "# mu2egrid manifest selfcheck: $sc" >> $manifest
}
#================================================================
# Ignores directories and other non-plain files
selectFiles() {
    for i in "$@"; do
        [ -f "$i" ] && [ ! -h "$i" ] && echo "$i"
    done
}
#================================================================
filterOutProxy() {
    for i in "$@"; do
        case "$i" in
            *.proxy)
            # Don't expose security sensitive info
                true;;
            *)
                echo "$i";;
        esac
    done
}
#================================================================
# The version of GNU time in SLF6 and SLF7 (/usr/bin/time) does not
# properly report when the supervised process is terminated by
# a signal.  We use a patched version instead if availalble.
resolve_timecmd() {
    for timecmd in \
    /cvmfs/mu2e.opensciencegrid.org/bin/SLF7/mu2e_time \
    /cvmfs/mu2e.opensciencegrid.org/bin/SLF6/mu2e_time \
    /usr/bin/time \
    ; do
        if $timecmd true > /dev/null 2>&1; then
            echo $timecmd
            return 0
        fi
    done

    # nothing works.  Do not fall back to the shell builtin
    # because its printout will fail mu2eClusterCheckAndMove
    echo "ERROR: resolve_timecmd: no compatible time command found." >&2
    return 1
}

#================================================================
mu2egrid_errh() {
    ret=$?
    if [ x"$MU2EGRID_ENV_PRINTED" == x ]; then
        echo "Dumping the environment on error"
        echo "#================================================================"
        /usr/bin/printenv
        echo "#================================================================"
        echo "End of the environment dump."
    fi
    echo "Error from $BASH_COMMAND: exit code $ret"
    echo "Sleeping for $error_delay seconds"
    sleep $error_delay
    exit $ret
}

#================================================================
# Run the framework jobs and create json files for the outputs
# Running it inside a function makes it easier to exit on error
# during the "payload" part, but still transfer the log file back.
mu2ejobsub_payload() {
    trap mu2egrid_errh ERR

    stagedir=$(pwd)/mu2egridInDir
    mkdir $stagedir

    mu2ejobiodetail --prestage-spec \
        --jobpar $jobdef --index $jobindex \
        --inspec $opsjson --stagedir $stagedir \
        > prestage_spec

    if [[ -s prestage_spec ]]; then
        echo "# prestage_spec follows:"
        cat prestage_spec
        echo "#----------------------------------------------------------------"
        echo "$(date) # Starting to pre-stage input files"
        type ifdh
        tstart=$(date +%s)
        ifdh cp -f prestage_spec
        ret=$?
        t2=$(date +%s)
        echo "$(date) # Total stage-in time: $((t2-tstart)) seconds, status $ret"
        echo "# ls -lR mu2egridInDir  on $(date) -- $(date +%s)"
        ls -lR mu2egridInDir
        echo "################################################################"
        echo ""
    fi
    rm -f prestage_spec

    localFCL=mu2e.fcl
    mu2ejobfcl --parfile $jobdef --index $jobindex \
        $(mu2ejobiodetail --cmdline --jobpar $jobdef --index $jobindex \
        --inspec $opsjson --stagedir $stagedir \
        ) > $localFCL

    #================================================================
    # Setup Mu2e Offline and run the job
    mu2ejobquery --extract-code $jobdef
    source `mu2ejobquery --setup $jobdef`

    # Prepend the working directory to MU2E_SEARCH_PATH, otherwise some pre-staged files
    # (e.g. custom stopped muon file) will not be found by mu2e modules.
    MU2E_SEARCH_PATH=$(pwd):$MU2E_SEARCH_PATH

    echo "#================================================================"
    echo "# mu2ejobsub $(date) -- $(date +%s) After Offline setup, the environment is:"
    /usr/bin/printenv
    export MU2EGRID_ENV_PRINTED=1
    echo "#================================================================"

    timecmd=$(resolve_timecmd)
    echo "mu2ejobsub $(date) -- $(date +%s) after timecmd resolution"


    #================================================================
    echo "################################################################"
    echo "# The content of the final fcl file begin"
    cat $localFCL
    echo "# The content of the final fcl file end"
    echo "################################################################"
    echo ""

    #================================================================
    # Run the job
    echo "Running the command: $timecmd mu2e -c $localFCL on  $(date) -- $(date +%s)"
    $timecmd mu2e -c $localFCL
    echo "mu2egrid exit status $? on $(date) -- $(date +%s)"

    echo "#================================================================"

    # Create metadata files for the outputs.

    rm -f parents
    mu2ejobquery --jobname $jobdef >> parents
    mu2ejobiodetail --inputs --jobpar $jobdef --index $jobindex >> parents

    declare -a manifestfiles=( )
    for i in $(mu2ejobiodetail --outputs --jobpar $jobdef --index $jobindex); do
        printJson.sh --parents parents $i > $i.json
        manifestfiles=(${manifestfiles[@]} $i $i.json)
    done
    rm -f parents

    if [[ "x$MU2EGRID_TRANSFER_ALL" == "x1" ]]; then
        # the log file needs a special treatment
        manifestfiles=( $(filterOutProxy $(selectFiles *) |grep -v $logFileName) )
    fi

    echo "mu2ejobsub $(date) -- $(date +%s) before addManifest"

    # After the manifest is created the log file must not be modified
    addManifest $logFileName "${manifestfiles[@]}" >&3 2>&4
}
export mu2ejobsub_payload

#================================================================
# Execution starts here

# Print this into the condor .out file; unlike the printinfo() output that goes into mu2e logs.
echo "Starting on host $(hostname) on $(date) -- $(date +%s) seconds since epoch"

[[ $MU2EGRID_DEBUG > 0 ]] && export IFDH_CP_MAXRETRIES=0
# Workaround for ifdh breakage on transfer timeout per INC000001147234
export IFDH_CP_UNLINK_ON_ERROR=1

[[ $MU2EGRID_DEBUG > 0 ]] && echo "We are in directory $(/bin/pwd)"
[[ $MU2EGRID_DEBUG > 0 ]] && /usr/bin/printenv

umask 002

# make sure we are not stuck with stale CVMFS data
CVMFSHACK=/cvmfs/grid.cern.ch/util/cvmfs-uptodate
test -x $CVMFSHACK && $CVMFSHACK /cvmfs/mu2e.opensciencegrid.org

ret=1
jobname=failedjob
export logFileName="${jobname}.log"
declare -a outfiles=( $logFileName )

# PROCESS is the original variable set by Condor
# Other systems call it differently, put it in PROCESS by hand.
PROCESS=${PROCESS:-$SLURM_PROCID}
PROCESS=${PROCESS:-$ALPS_APP_PE}
export PROCESS

cluster=$(printf %06d ${CLUSTER:-0})
clustername="${cluster}${MU2EGRID_CLUSTERNAME:+.$MU2EGRID_CLUSTERNAME}"
[[ $MU2EGRID_HPC ]] && clustername=${MU2EGRID_CLUSTERNAME}
finalOutDir="${MU2EGRID_WFOUTSTAGE:?Error: MU2EGRID_WFOUTSTAGE is not set}/$clustername/$(printf %02d $((${PROCESS:-0}/1000)))/$(printf %05d ${PROCESS:-0})"

if [[ $MU2EGRID_HPC ]]; then
    mkdir -p ${finalOutDir}
    cd ${finalOutDir}
    export TMPDIR=${finalOutDir}
else
    # TMPDIR is defined and created by Condor.
    cd $TMPDIR
fi

#================================================================
# Test for using tokens during the proxy-token transition period

if [[ $MU2EGRID_DELETE_PROXY ]]; then
    # This printout goes to jobsub logs, not the mu2e job log
    echo "Deleting X509 proxy $X509_USER_PROXY"
    rm -f $X509_USER_PROXY
    unset X509_USER_PROXY

    echo "# Running httokendecode ================"
    httokendecode
    echo "# After httokendecode ================"
fi

#================================================================
# Set up Mu2e environment and make ifdh available
if source "${MU2EGRID_MU2ESETUP:?Error: MU2EGRID_MU2ESETUP: not defined}"; then

    setup -B mu2ejobtools $MU2EGRID_MU2EJOBTOOLS_VERSION
    if mu2ejobquery --help >/dev/null 2>$errfile; then
        rm -f $errfile

        if [[ ! $MU2EGRID_HPC ]]; then
            setup -B ifdhc $IFDH_VERSION
        fi

        if [[ $MU2EGRID_HPC ]] || (ifdh --help > /dev/null 2> $errfile ); then
            rm -f $errfile

            printinfo >> $logFileName 2>&1

            jobdef="$CONDOR_DIR_INPUT/${MU2EGRID_JOBDEF:?MU2EGRID_JOBDEF: environment variable is not set}";
            export jobdef
            echo "mu2ejobsub jobdef = $jobdef" >> $logFileName 2>&1

            opsjson="$CONDOR_DIR_INPUT/${MU2EGRID_OPSJSON:?MU2EGRID_OPSJSON: environment variable is not set}";
            export opsjson
            echo "mu2ejobsub opsjson = $opsjson" >> $logFileName 2>&1

            jobindex=$(mu2ejobmap --clusterpars $opsjson --process $PROCESS)
            echo "mu2ejobsub jobindex = $jobindex" >> $logFileName 2>&1

            newLogFileName=$(mu2ejobiodetail --logfile --jobpar $jobdef --index $jobindex)
            /bin/mv -f "$logFileName" "$newLogFileName"
            export logFileName=$newLogFileName

            echo "mu2ejobsub $(date) -- $(date +%s) before the payload" >> $logFileName 2>&1
            ( mu2ejobsub_payload ) 3>&1 4>&2 1>> $logFileName 2>&1
            ret=$?

            # The log file should not be touched after payload exit.  Further messages go to jobsub/condor logs.
            echo "mu2ejobsub $(date) -- $(date +%s) after the payload"

            outfiles=( $logFileName )
            for i in $(mu2ejobiodetail --outputs --jobpar $jobdef --index $jobindex ); do
                outfiles=( ${outfiles[@]} $i $i.json )
            done

            if [[ "x$MU2EGRID_TRANSFER_ALL" == "x1" ]]; then
                shopt -u failglob
                shopt -s nullglob
                outfiles=( $(filterOutProxy $(selectFiles *) ) )
            fi

            if ! [[ $MU2EGRID_HPC ]]; then

                # Transfer the results.  There were cases when jobs failed after
                # creating the outstage directory, and were automatically restarted by
                # condor.  I also observed cases when more than one instance of the
                # same job, duplicated by some glitches in the grid system, completed
                # and transferred files back.  To prevent data corruption we write to
                # a unique tmp dir, than rename it to the final name.

                tmpOutDir="${finalOutDir}.$(od -A n -N 4 -t x4 /dev/urandom|sed -e 's/ //g')"

                echo "mu2ejobsub $(date) -- $(date +%s) before calling ifdh outstage"

                t1=$(date +%s)

                ifdh mkdir_p ${MU2EGRID_IFDHEXTRAOPTS} ${tmpOutDir}
                ifdhret=$?
                if [[ $ifdhret -ne 0 ]]; then
                    echo "The command: ifdh mkdir_p ${MU2EGRID_IFDHEXTRAOPTS} ${tmpOutDir}" >&2
                    echo "has failed on $(date) with status code $ifdhret.  Re-running with IFDH_DEBUG=10." >&2
                    IFDH_DEBUG=10 ifdh mkdir_p ${MU2EGRID_IFDHEXTRAOPTS} ${tmpOutDir} >&2
                    ifdhret=$?
                fi

                if [[ $ifdhret -eq 0 ]]; then
                    ifdh cp $MU2EGRID_IFDHEXTRAOPTS -D "${outfiles[@]}" "${tmpOutDir}"
                    ifdhret=$?

                    if [[ $ifdhret -ne 0 ]]; then
                        echo "The command: ifdh cp ${MU2EGRID_IFDHEXTRAOPTS} -D ${outfiles[@]} ${tmpOutDir}" >&2
                        echo "has failed on $(date) with status code $ifdhret.  Re-running with IFDH_DEBUG=10." >&2
                        IFDH_DEBUG=10 ifdh cp ${MU2EGRID_IFDHEXTRAOPTS} -D "${outfiles[@]}" "${tmpOutDir}" >&2
                        ifdhret=$?
                    fi
                fi

                if [[ ( $ret -eq 0 ) && ( $ifdhret -ne 0 ) ]]; then
                    echo "outstage failed on $(date): exit code $ifdhret from ifdh" >&2
                    ret=23
                fi

                if [[ $ifdhret -eq 0 ]]; then
                    # ignore exit codes here - we've got the files
                    ifdh rename ${MU2EGRID_IFDHEXTRAOPTS} "${tmpOutDir}" "${finalOutDir}"

                    t2=$(date +%s)
                    echo "$(date) # Total outstage time: $((t2-t1)) seconds"
                fi
            fi

        else
            savederr=$?
            exec 1>&2
            printinfo "Error report"
            echo "#================================================================"
            echo "# The environment is:"
            /usr/bin/printenv
            echo "#================================================================"
            echo "Error when setting up ifdh: exit code $savederr"
            echo "The error message was:"
            cat $errfile
            echo ""
            echo "Sleeping for $error_delay seconds"
            sleep $error_delay
            ret=19
        fi
    else
        savederr=$?
        exec 1>&2
        printinfo "Error report"
        echo "#================================================================"
        echo "# The environment is:"
        /usr/bin/printenv
        echo "#================================================================"
        echo "Error when setting up mu2ejobtools: exit code $savederr"
        echo "The error message from 'mu2ejobquery -h' was:"
        cat $errfile
        echo ""
        echo "Sleeping for $error_delay seconds"
        sleep $error_delay
        ret=19
    fi
else
    savederr=$?
    exec 1>&2
    printinfo "Error report"
    echo "#================================================================"
    echo "# The environment is:"
    /usr/bin/printenv
    echo "#================================================================"
    echo "Error sourcing setup script ${MU2EGRID_MU2ESETUP}: status code $savederr"
    echo "Sleeping for $error_delay seconds"
    sleep $error_delay
    ret=18
fi

exit $ret
