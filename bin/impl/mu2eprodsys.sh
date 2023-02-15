#!/bin/bash
#
# Grid worker node scrip for mu2e framework jobs.  It is not meant to
# be invoked by users directly.  One should use the mu2eprodsys user
# interface program to prepare the environment and submit this script.
#
# Andrei Gaponenko, 2014, 2015
#

error_delay="${MU2EGRID_ERRORDELAY:?Error: MU2EGRID_ERRORDELAY is not defined}"
errfile=$TMPDIR/mu2eprodsys_errmsg.$$

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
    echo "#================================================================"
}
#================================================================
# Extract the name of the fcl file to work on from the file list
getFCLFileName() {
    masterlist=${1:?getFCLFileName: masterlist arg missing}
    process=${2:?getFCLFileName:: process arg missing}
    chunksize=1
    firstline=$((1 + $chunksize * $process))
    tail --lines=+"$firstline" "$masterlist" | head --lines="$chunksize"
}
#================================================================
formatInputFileSpec() {
    filespec="${1:?formatInputFileSpec: filespec arg is missing}"
    # filespec format is "/pfs/file/name.art  local/file/name.art"
    if [[ x"$MU2EGRID_XROOTD" == x1 ]]; then
        # Derive xrootd URLs from pnfs file names
        awk 'BEGIN{first=1}; {if(!first) {cm=",";} else {first=0; cm=" ";}; fn=$1; sub("/pnfs/", "xroot://fndca1.fnal.gov/pnfs/fnal.gov/usr/",fn); print "    "cm"\""fn"\""}' $filespec
    else
        # Use local file names.
        awk 'BEGIN{first=1}; {if(!first) {cm=",";} else {first=0; cm=" ";}; print "    "cm"\""$2"\""}' "$filespec"
    fi
}

#================================================================
addManifest() {
    manifest=${1:?addManifest: logFileName is missing}
    shift
    echo "mu2eprodsys diskUse = $(du -ks)" >> $manifest
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
set_corsika_parameter() {
    trap mu2egrid_errh ERR
    local file="$1"
    local name="$2"
    local value="$3"

    if awk '{if(gsub(/^'$name' .*$/, "'$name'      '"$value"'")){++count;}; print};
             END { if(!count) { exit(1); }
             }' $file > $file.tmp;
    then
        mv -f $file.tmp $file
    else
        echo "Error updating CORSIKA config $file: did not match parameter $name";
        return 1
    fi
}
#================================================================
write_corsika_config() {
    trap mu2egrid_errh ERR

    local target=$1
    shift

    if source "${MU2EGRID_USERSETUP:?Error: MU2EGRID_USERSETUP: not defined}"; then
        # Prepend the working directory to MU2E_SEARCH_PATH, otherwise some pre-staged files
        # (e.g. custom stopped muon file) will not be found by mu2e modules.
        MU2E_SEARCH_PATH=$(pwd):$MU2E_SEARCH_PATH
        if [[ $MU2EGRID_MU2EBINTOOLS_VERSION ]]; then
            setup -B mu2ebintools "${MU2EGRID_MU2EBINTOOLS_VERSION}" -q "${MU2E_UPS_QUALIFIERS}"
        fi
        setup -B mu2etools "${MU2EGRID_MU2ETOOLS_VERSION}"

        # Note: we use the subrun number for corsika
        local subrun=$(fhicl-get --atom-as int mu2emetadata.firstSubRun $localFCL)
        set_corsika_parameter $target  RUNNR $subrun

        # The actual run number is used by the source module
        local run=$(fhicl-get --atom-as int mu2emetadata.firstRun $localFCL)
        echo "source.runNumber: $run" >> $localFCL

        local NSHOW=$(fhicl-get --atom-as int mu2emetadata.maxEvents $localFCL)
        set_corsika_parameter $target  NSHOW $NSHOW

        local SEED=$(fhicl-get --atom-as int services.SeedService.baseSeed $localFCL)
        set_corsika_parameter $target  SEED "$SEED 0 0"

    else
        echo "write_corsika_config: failed to source $MU2EGRID_USERSETUP"
        false
    fi
}

#================================================================
prerun_corsika() {
    trap mu2egrid_errh ERR
    echo "mu2egrid: executing ${FUNCNAME[0]}"

    local prconf="$CONDOR_DIR_INPUT/${MU2EGRID_PRCONF:?Error: MU2EGRID_PRCONF not defined in ${FUNCNAME[0]}}"

    # Setup packages per config file instructions
    local setupfile=prerunsetup
    awk '{if(gsub(/^[cC\*] *mu2egrid  *setup:/, "")) print}' "$prconf" |\
    while read line; do
        echo echo "mu2egrid: Performing setup $line" >> $setupfile
        echo setup $line >> $setupfile
    done
    source $setupfile
    rm -f $setupfile

    # Extract the name of the CORSIKA executable
    local corsika=$(awk '{if(gsub(/^[cC\*] *mu2egrid  *executable:/, "")) print}' $prconf)

    # Set parameters for this individual CORSIKA run: SEED, NSHOW, RUNNR
    # We need to setup mu2ebintools to extract info from localFCL
    # Do it in a subshell to avoid potential conflict with CORSIKA dependencies.
    ( write_corsika_config $prconf )

    echo "#================================================================"
    echo "mu2egrid: Final CORSIKA configuration:"
    cat $prconf
    echo "#================================================================"

    echo "mu2egrid: Starting $corsika on $(date)"
    $(resolve_timecmd) $corsika < $prconf

    local origBin=$(ls DAT*)

    #FIXME: # Derive Mu2e name for the CORSIKA bin file from this job's FCL file.
    #FIXME: local mu2eBin=$(echo $localFCL|sed -e 's|^\(\./\)\?cnf|sim|' -e 's/fcl$/csk/')
    #FIXME: mv $origBin $mu2eBin
    #FIXME:
    #FIXME: echo 'source.fileNames: ["'$mu2eBin'"]' >> $localFCL
    #FIXME:
    # The FromCorsikaBinary source in Offline v09_03_00 is too picky
    # to use $mu2eBin name.
    echo 'source.fileNames: ["'$origBin'"]' >> $localFCL

    # The preprocessing is done, flip the safety
    echo "mu2emetadata.ignoreSource: 0" >> $localFCL

    echo "mu2egrid: end of ${FUNCNAME[0]}"
}
#================================================================
# Run the framework jobs and create json files for the outputs
# Running it inside a function makes it easier to exit on error
# during the "payload" part, but still transfer the log file back.
mu2eprodsys_payload() {
    trap mu2egrid_errh ERR

    [[ $MU2EGRID_HPC ]] || mkdir mu2egridInDir

    if [ -n "$MU2EGRID_FCLTAR" ]; then

       # fcl files were given to this job as a tarball, we need to extract our job config
        echo "mu2eprodsys $(date) -- $(date +%s) FCL files are given as a tar file: $MU2EGRID_FCLTAR"

        if [[ $MU2EGRID_HPC ]]; then
            tar --extract --transform='s|^.*/||' --file "$MU2EGRID_FCLTAR" $origFCL
            # these names may coincide
            [[ $(basename $origFCL) == $localFCL ]] || /bin/mv -v $(basename $origFCL) $localFCL
        else
            localTar=mu2egridInDir/$(basename $MU2EGRID_FCLTAR)
            ifdh cp "$MU2EGRID_FCLTAR" $localTar
            tar xf $localTar --directory mu2egridInDir $origFCL
            rm -v $localTar
            mv -v mu2egridInDir/$origFCL $localFCL
        fi

        echo "mu2eprodsys $(date) -- $(date +%s) after FCL file extraction"

    else # Job submissions with plain fcl file list

        echo "mu2eprodsys $(date) -- $(date +%s) Copying in $origFCL"
        ifdh cp $origFCL $localFCL
        echo "mu2eprodsys $(date) -- $(date +%s) after FCL file retrieval"

    fi

    #================================================================
    # Handle the code-in-the-user-tarball case.
    if [ -n "$MU2EGRID_CODE" ]; then
        MU2EGRID_USERSETUP="${INPUT_TAR_DIR}/${MU2EGRID_USERSETUP}";
        echo "mu2eprodsys $(date) -- $(date +%s) pointing MU2EGRID_USERSETUP to the tarball extract: $MU2EGRID_USERSETUP"
    fi

    #================================================================
    # Perform a "prerun" processing if requested.  Use a subshell for different product setup.

    if [ -n "$MU2EGRID_PRERUN" ]; then
        ( prerun_"$MU2EGRID_PRERUN" )
    fi

    #================================================================

    if source "${MU2EGRID_USERSETUP:?Error: MU2EGRID_USERSETUP: not defined}"; then

        echo "mu2eprodsys $(date) -- $(date +%s) after sourcing $MU2EGRID_USERSETUP"

        # Prepend the working directory to MU2E_SEARCH_PATH, otherwise some pre-staged files
        # (e.g. custom stopped muon file) will not be found by mu2e modules.
        MU2E_SEARCH_PATH=$(pwd):$MU2E_SEARCH_PATH

        if [[ $MU2EGRID_MU2EBINTOOLS_VERSION ]]; then
            setup -B mu2ebintools "${MU2EGRID_MU2EBINTOOLS_VERSION}" -q "${MU2E_UPS_QUALIFIERS}"
        fi

        if [[ $MU2EGRID_HPC ]]; then
            # Package version inside container can not be determined at submission time.
            # We use the container's "current" here, but the container itself is versioned.
            setup mu2efilename
            setup mu2etools
        else
            setup -B mu2efilename "${MU2EGRID_MU2EFILENAME_VERSION}"
            setup -B mu2etools "${MU2EGRID_MU2ETOOLS_VERSION}"
        fi

        echo "#================================================================"
        echo "# mu2eprodsys $(date) -- $(date +%s) After package setup, the environment is:"
        /usr/bin/printenv
        export MU2EGRID_ENV_PRINTED=1
        echo "#================================================================"

        #================================================================
        # Got mu2ebintools.  Check that we did not get an untreated
        # fcl made with --ignore-source
        if fhicl-get --names-in mu2emetadata $localFCL | grep -q ignoreSource; then
            ignoreSource="$(fhicl-get --atom-as int mu2emetadata.ignoreSource $localFCL)"
            if (( $ignoreSource )); then
                echo "ERROR: unexpected value ignoreSource=$ignoreSource.  Input fcl needs extra processing before we get here."
                exit 1
            fi
        fi

        #================================================================
        timecmd=$(resolve_timecmd)
        echo "mu2eprodsys $(date) -- $(date +%s) after timecmd resolution"

        #================================================================
        # Pre-stage input data files, and write their SAM names
        # to the "parents" file for later use

        touch localFileDefs
        # invoke fhicl-get as a separate command outside of for...done so that errors are trapped
        keys=( $(fhicl-get --sequence-of string mu2emetadata.fcl.inkeys $localFCL) )
        for key in "${keys[@]}"; do

            rfns=( $(fhicl-get --sequence-of string $key $localFCL ) )
            for rfn in "${rfns[@]}"; do
                # copy it to mu2egridInDir
                bn="$(basename $rfn)"
                lfn="mu2egridInDir/$bn"
                echo $rfn $lfn >> tmpspec
                echo $bn >> parents
            done

            echo "$key : [" >> localFileDefs
            formatInputFileSpec tmpspec >> localFileDefs
            echo "]" >> localFileDefs

            cat tmpspec >> prestage_spec
            rm tmpspec
        done

        echo "mu2eprodsys $(date) -- $(date +%s) after mu2emetadata.fcl.inkeys processing"

        # Handle input files defined in fhicl prolog variables

        # Most CD3 fcl datasets do not have mu2emetadata.fcl.prologkeys defined.
        # A workaround for backward compatibility: check that the variable is present
        # before trying to query its value.
        metalist=( $(fhicl-get --names-in mu2emetadata.fcl $localFCL) )
        keys=()
        for i in "${metalist[@]}"; do
            case $i in
                prologkeys) keys=( $(fhicl-get --sequence-of string mu2emetadata.fcl.prologkeys $localFCL) )
            esac
        done

        touch prologFileDefs
        for key in "${keys[@]}"; do

            rfns=( $(fhicl-get --sequence-of string "mu2emetadata.fcl.prolog_values.$key" $localFCL ) )
            for rfn in "${rfns[@]}"; do
                # copy it to mu2egridInDir
                bn="$(basename $rfn)"
                lfn="mu2egridInDir/$bn"
                echo $rfn $lfn >> tmpspec
                echo $bn >> parents
            done

            echo "BEGIN_PROLOG # by mu2eprodsys" >> prologFileDefs
            echo "$key @protect_ignore: [" >> prologFileDefs
            formatInputFileSpec tmpspec >> prologFileDefs
            echo "]" >> prologFileDefs
            echo "END_PROLOG # by mu2eprodsys" >> prologFileDefs

            cat tmpspec >> prestage_spec
            rm tmpspec
        done

        echo "mu2eprodsys $(date) -- $(date +%s) after mu2emetadata.fcl.prologkeys processing"

        if [[ -e prestage_spec ]] && [[ x"$MU2EGRID_XROOTD" != x1  ]] && [[ x"$MU2EGRID_NO_PRESTAGE" == x ]]; then
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
        fi
        rm -f prestage_spec

        #================================================================
        echo "#----------------------------------------------------------------" >> $localFCL
        echo "# code added by mu2eprodys" >> $localFCL

        if [[ x"$MU2EGRID_NO_PRESTAGE" == x ]]; then
            # Point job to the input files

            cat prologFileDefs ${localFCL} > ${localFCL}.tmp
            mv -f ${localFCL}.tmp ${localFCL}

            cat localFileDefs >> $localFCL
        fi
        rm -f prologFileDefs localFileDefs

        #----------------------------------------------------------------
        # set output file names
        keys=( $(fhicl-get --sequence-of string mu2emetadata.fcl.outkeys $localFCL ) )
        for key in "${keys[@]}"; do
            oldname=$(fhicl-get --atom-as string $key $localFCL)
            newname=$(echo $oldname| awk -F . '{OFS="."; $2="'${MU2EGRID_DSOWNER:?"Error: MU2EGRID_DSOWNER is not set"}'"; $4="'${MU2EGRID_DSCONF}'"; print $0;}')
            echo "$key : \"$newname\"" >> $localFCL
        done

        #----------------------------------------------------------------
        # Handle multithreading.
        # FIXME: this should be revised to enable general support (including
        # on OSG) when we upgrade to art3.   The mu2eprodsys submission
        # script will need to implement suitable new options.
        #
        # For the moment just let the HPC scripts handle G4-specific
        # multithreading with art2.

        if [[ $MU2EGRID_FCLMT ]]; then
            echo "# extra settings for the HPC environment" >> $localFCL
            cat $MU2EGRID_FCLMT >> $localFCL
        fi

        #----------------------------------------------------------------
        echo "# end code added by mu2eprodys" >> $localFCL
        echo "#----------------------------------------------------------------" >> $localFCL

        echo "mu2eprodsys $(date) -- $(date +%s) job FCL file finalized"

        #================================================================
        # Document what has been actually pre-staged
        if ! [[ $MU2EGRID_HPC ]]; then
            echo "################################################################"
            echo "# ls -lR mu2egridInDir  on $(date) -- $(date +%s)"
            ls -lR mu2egridInDir
            echo ""
        fi

        #================================================================
        # include the edited copy of the fcl into the log
        echo "################################################################"
        echo "# The content of the final fcl file begin"
        cat $localFCL
        echo "# The content of the final fcl file end"
        echo "################################################################"

        #================================================================
        # Run the job
        echo "Running the command: $timecmd mu2e -c $localFCL on  $(date) -- $(date +%s)"
        $timecmd mu2e -c $localFCL
        echo "mu2egrid exit status $? on $(date) -- $(date +%s)"

        echo "#================================================================"

        # Create SAM metadata for the outputs.

        case ${MU2EGRID_DSOWNER} in
            mu2e*) ffprefix=phy ;;
            *)     ffprefix=usr ;;
        esac

        echo $(basename $origFCL) >> parents

        shopt -u failglob
        shopt -s nullglob

        for i in *.art *.root; do
            printJson.sh --parents parents $i > $i.json
        done

        rm -f parents

        declare -a manifestfiles=( *.art *.root *.json )
        if [[ "x$MU2EGRID_TRANSFER_ALL" == "x1" ]]; then
            # the log file needs a special treatment
            manifestfiles=( $(filterOutProxy $(selectFiles *) |grep -v $logFileName) )
        fi

        echo "mu2eprodsys $(date) -- $(date +%s) before addManifest"

        # After the manifest is created the log file must not be modified
        addManifest $logFileName "${manifestfiles[@]}" >&3 2>&4

    else
        echo "Error sourcing setup script ${MU2EGRID_USERSETUP}: status code $?"
        echo "Sleeping for $error_delay seconds"
        sleep $error_delay
        exit 21
    fi
}
export mu2eprodsys_payload

#================================================================
# Execution starts here

# Print this into the condor .out file; unlike the printinfo() output that goes into mu2e logs.
echo "Starting on host $(hostname) on $(date) -- $(date +%s) seconds since epoch"

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
PROCESS=${PROCESS:-$MU2EGRID_PROCID}
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
# Set up Mu2e environment and make ifdh available
if source "${MU2EGRID_MU2ESETUP:?Error: MU2EGRID_MU2ESETUP: not defined}"; then

    if [[ ! $MU2EGRID_HPC ]]; then
        setup -B ifdhc $IFDH_VERSION

        # Can not use -B because we are overriding a package already
        # setup by ifdhc above
        setup ifdhc_config "${MU2EGRID_IFDHC_CONFIG_VERSION}"
    fi

    if [[ $MU2EGRID_HPC ]] || ( type ifdh 2> $errfile ); then

        rm -f $errfile

        printinfo >> $logFileName 2>&1

        masterlist="$CONDOR_DIR_INPUT/${MU2EGRID_INPUTLIST:?MU2EGRID_INPUTLIST environment variable is not set}";
        export origFCL=$(getFCLFileName $masterlist ${PROCESS:?PROCESS environment variable is not set}) 2>> $logFileName

        echo "mu2eprodsys origFCL = $origFCL" >> $logFileName 2>&1

        if [ -n "$origFCL" ]; then

            # set current user and version info to obtain the name of this job
            jobname=$(basename $origFCL .fcl | awk -F . '{OFS="."; $2="'${MU2EGRID_DSOWNER:?"Error: MU2EGRID_DSOWNER is not set"}'"; $4="'${MU2EGRID_DSCONF}'"; print $0;}')
            newLogFileName=$(echo $jobname|awk -F . '{OFS="."; $1="log"; print $0;}').log
            mv "$logFileName" "$newLogFileName"
            export logFileName=$newLogFileName

            export localFCL="./$jobname.fcl"

            echo "mu2eprodsys $(date) -- $(date +%s) before the payload" >> $logFileName 2>&1
            ( mu2eprodsys_payload ) 3>&1 4>&2 1>> $logFileName 2>&1
            ret=$?

            # The log file should not be touched after payload exit.  Further messages go to jobsub/condor logs.
            echo "mu2eprodsys $(date) -- $(date +%s) after the payload"

            shopt -u failglob
            shopt -s nullglob
            outfiles=( $logFileName *.art *.root *.json )
            if [[ "x$MU2EGRID_TRANSFER_ALL" == "x1" ]]; then
                outfiles=( $(filterOutProxy $(selectFiles *) ) )
            fi
        fi

        if ! [[ $MU2EGRID_HPC ]]; then

            # Transfer the results.  There were cases when jobs failed after
            # creating the outstage directory, and were automatically restarted by
            # condor.  I also observed cases when more than one instance of the
            # same job, duplicated by some glitches in the grid system, completed
            # and transferred files back.  To prevent data corruption we write to
            # a unique tmp dir, than rename it to the final name.

            tmpOutDir="${finalOutDir}.$(od -A n -N 4 -t x4 /dev/urandom|sed -e 's/ //g')"

            echo "mu2eprodsys $(date) -- $(date +%s) before calling ifdh outstage"

            t1=$(date +%s)

            ifdh mkdir_p ${MU2EGRID_IFDHEXTRAOPTS} ${OUTDIR}
            ifdhret=$?
            if [[ $ifdhret -ne 0 ]]; then
                echo "The command: ifdh mkdir_p ${MU2EGRID_IFDHEXTRAOPTS} ${OUTDIR}" >&2
                echo "has failed on $(date) with status code $ifdhret.  Re-running with IFDH_DEBUG=10." >&2
                IFDH_DEBUG=10 ifdh mkdir_p ${MU2EGRID_IFDHEXTRAOPTS} ${OUTDIR} >&2
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
    echo "Error sourcing setup script ${MU2EGRID_MU2ESETUP}: status code $savederr"
    echo "Sleeping for $error_delay seconds"
    sleep $error_delay
    ret=18
fi

exit $ret
