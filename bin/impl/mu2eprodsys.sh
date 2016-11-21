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
    echo ${1:-Starting} on host `uname -a` on `date`
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
addManifest() {
    manifest=${1:?addManifest: }
    shift
    echo "mu2eprodsys diskUse = $(du -ks)" >> $manifest
    echo '#================================================================' >> $manifest
    echo '# mu2egrid manifest' >> $manifest
    ls -alR |awk '{print "# "$0}' >> $manifest
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
# Run the framework jobs and create json files for the outputs
# Running it inside a function makes it easier to exit on error
# during the "payload" part, but still transfer the log file back.
mu2eprodsys_payload() {

    mu2epseh() {
        ret=$?
        echo "Error from $BASH_COMMAND: exit code $ret"
        echo "Sleeping for $error_delay seconds"
        sleep $error_delay
        exit $ret
    }
    trap mu2epseh ERR

    echo "Copying in $origFCL"
    ifdh cp $origFCL $localFCL

    #================================================================

    if source "${MU2EGRID_USERSETUP:?Error: MU2EGRID_USERSETUP: not defined}"; then

        setup -B mu2ebintools "${MU2EGRID_MU2EBINTOOLS_VERSION:?Error: MU2EGRID_MU2EBINTOOLS_VERSION is not set}" -q "${MU2E_UPS_QUALIFIERS}"
        setup -B sam_web_client "${MU2EGRID_SAM_WEB_CLIENT_VERSION:?Error: MU2EGRID_SAM_WEB_CLIENT_VERSION is not set}"
        setup -B dhtools "${MU2EGRID_DHTOOLS_VERSION:?Error: MU2EGRID_DHTOOLS_VERSION is not set}"

        echo "#================================================================"
        echo "# After package setup, the environment is:"
        /usr/bin/printenv
        echo "#================================================================"

        # The version of GNU time in SLF6 (/usr/bin/time) does not
        # properly report when the supervised process is terminated by
        # a signal.  We use a patched version instead.

        # FIXME: package mu2e_time as a UPS product.
        #
        # There is a copy of GNU time on CVMFS, but not as a UPS
        # package.  We'd need to re-implement a part of UPS to
        # select the correct binary to run on the current node.
        # Instead just try to run the SL6 version and use it if
        # successful.

        timecmd=time  # shell builtin is the fallback option

        mu2etime=/cvmfs/mu2e.opensciencegrid.org/bin/SLF6/mu2e_time
        if $mu2etime true > /dev/null 2>&1; then
            timecmd=$mu2etime;
        fi

        #================================================================
        # Pre-stage input data files, and write their SAM names
        # to the "parents" file for later use

        mkdir mu2egridInDir
        touch localFileDefs
        # invoke fhicl-getpar as a separate command outside of for...done so that errors are trapped
        keys=( $(fhicl-getpar --strlist mu2emetadata.fcl.inkeys $localFCL) )
        for key in "${keys[@]}"; do

            rfns=( $(fhicl-getpar --strlist $key $localFCL ) )
            for rfn in "${rfns[@]}"; do
                # copy it to mu2egridInDir
                bn="$(basename $rfn)"
                lfn="mu2egridInDir/$bn"

                # leave alone absolute path names, but expand SAM file names for ifdh
                if [[ $rfn != '/'* ]]; then
                    rfn="$(samweb get-file-access-url $rfn)"
                fi

                echo $rfn $lfn >> tmpspec
                echo $bn >> parents
            done

            echo "$key : [" >> localFileDefs
            awk 'BEGIN{first=1}; {if(!first) {cm=",";} else {first=0; cm=" ";}; print "    "cm"\""$2"\""}' tmpspec >> localFileDefs
            echo "]" >> localFileDefs

            cat tmpspec >> prestage_spec
            rm tmpspec
        done

        # Handle input files defined in fhicl prolog variables

        # Most CD3 fcl datasets do not have mu2emetadata.fcl.prologkeys defined.
        # A workaround for backward compatibility: check that the variable is present
        # before trying to query its value.
        metalist=( $(fhicl-getpar --keys mu2emetadata.fcl $localFCL) )
        keys=()
        for i in "${metalist[@]}"; do
            case $i in
                prologkeys) keys=( $(fhicl-getpar --strlist mu2emetadata.fcl.prologkeys $localFCL) )
            esac
        done

        touch prologFileDefs
        for key in "${keys[@]}"; do

            rfns=( $(fhicl-getpar --strlist "mu2emetadata.fcl.prolog_values.$key" $localFCL ) )
            for rfn in "${rfns[@]}"; do
                # copy it to mu2egridInDir
                bn="$(basename $rfn)"
                lfn="mu2egridInDir/$bn"

                # leave alone absolute path names, but expand SAM file names for ifdh
                if [[ $rfn != '/'* ]]; then
                    rfn="$(samweb get-file-access-url $rfn)"
                fi

                echo $rfn $lfn >> tmpspec
                echo $bn >> parents
            done

            echo "BEGIN_PROLOG # by mu2eprodsys" >> prologFileDefs
            echo "$key @protect_ignore: [" >> prologFileDefs
            awk 'BEGIN{first=1}; {if(!first) {cm=",";} else {first=0; cm=" ";}; print "    "cm"\""$2"\""}' tmpspec >> prologFileDefs
            echo "]" >> prologFileDefs
            echo "END_PROLOG # by mu2eprodsys" >> prologFileDefs

            cat tmpspec >> prestage_spec
            rm tmpspec
        done

        if [[ -e prestage_spec ]] && [[ x"$MU2EGRID_NO_PRESTAGE" == x ]]; then
            echo "# prestage_spec follows:"
            cat prestage_spec
            echo "#----------------------------------------------------------------"
            echo "$(date) # Starting to pre-stage input files"
            type ifdh
            tstart=$(date +%s)
            ifdh cp -f prestage_spec
            t2=$(date +%s)
            echo "$(date) # Total stage-in time: $((t2-tstart)) seconds, status $ret"


            # Point fcl to pre-staged file -  only if we do prestage

            # FIXME: remove 'sed' after https://cdcvs.fnal.gov/redmine/issues/12877
            # is resolved.  Note that the workaround does not work
            # on @protect_ignore in included files.  But it is still
            # a useful hack for now.
            sed -e 's/@protect_ignore:/:/g' $localFCL > ${localFCL}.tmp
            cat prologFileDefs ${localFCL}.tmp > $localFCL
            rm ${localFCL}.tmp

            # set input file names
            echo "#----------------------------------------------------------------" >> $localFCL
            echo "# code added by mu2eprodys" >> $localFCL
            cat localFileDefs >> $localFCL
        fi

        # set output file names
        keys=( $(fhicl-getpar --strlist mu2emetadata.fcl.outkeys $localFCL ) )
        for key in "${keys[@]}"; do
            oldname=$(fhicl-getpar --string $key $localFCL)
            newname=$(echo $oldname| awk -F . '{OFS="."; $2="'${MU2EGRID_DSOWNER:?"Error: MU2EGRID_DSOWNER is not set"}'"; $4="'${MU2EGRID_DSCONF}'"; print $0;}')
            echo "$key : \"$newname\"" >> $localFCL
        done

        echo "# end code added by mu2eprodys" >> $localFCL
        echo "#----------------------------------------------------------------" >> $localFCL

        #================================================================
        # include the edited copy of the fcl into the log
        echo "################################################################"
        echo "# The content of the final fcl file begin"
        cat $localFCL
        echo "# The content of the final fcl file end"
        echo "################################################################"

        #================================================================
        # Run the job
        echo "Running the command: $timecmd mu2e -c $localFCL"
        $timecmd mu2e -c $localFCL
        echo "mu2egrid exit status $?"

        echo "#================================================================"

        # Create SAM metadata for the outputs.

        case ${MU2EGRID_DSOWNER} in
            mu2e*) ffprefix=phy ;;
            *)     ffprefix=usr ;;
        esac

        echo $(basename $origFCL) >> parents

        shopt -u failglob
        shopt -s nullglob

        generator_type=$(fhicl-getpar --string mu2emetadata.mc.generator_type $localFCL)
        simulation_stage=$(fhicl-getpar --int    mu2emetadata.mc.simulation_stage $localFCL)
        primary_particle=$(fhicl-getpar --string mu2emetadata.mc.primary_particle $localFCL)

        for i in *.art; do
            jsonMaker.py \
                -f ${ffprefix}-sim \
                -a parents \
                -i mc.generator_type=$generator_type \
                -i mc.simulation_stage=$simulation_stage \
                -i mc.primary_particle=$primary_particle \
                -x \
                $i
        done

        for i in *.root; do
            jsonMaker.py \
                -f ${ffprefix}-nts \
                -a parents \
                -i mc.generator_type=$generator_type \
                -i mc.simulation_stage=$simulation_stage \
                -i mc.primary_particle=$primary_particle \
                -x \
                $i
        done

        declare -a manifestfiles=( *.art *.root *.json )

        # A file should be immutable after its json is created.
        # addManifest appends to the log file; log.json has to be made after that.
        addManifest $logFileName "${manifestfiles[@]}" >&3 2>&4

        for i in $logFileName; do
            jsonMaker.py \
                -f ${ffprefix}-etc \
                -a parents \
                -i mc.generator_type=$generator_type \
                -i mc.simulation_stage=$simulation_stage \
                -i mc.primary_particle=$primary_particle \
                -x \
                $i >&3 2>&4
        done

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

umask 002

# TMPDIR is defined and created by Condor.
cd $TMPDIR

# make sure we are not stuck with stale CVMFS data
CVMFSHACK=/cvmfs/grid.cern.ch/util/cvmfs-uptodate
test -x $CVMFSHACK && $CVMFSHACK /cvmfs/mu2e.opensciencegrid.org

ret=1
cluster=$(printf %06d ${CLUSTER:-0})
clustername="$cluster${MU2EGRID_CLUSTERNAME:+.$MU2EGRID_CLUSTERNAME}"

jobname=failedjob
export logFileName="${jobname}.log"
declare -a outfiles=( $logFileName )
finalOutDir="${MU2EGRID_WFOUTSTAGE:?Error: MU2EGRID_WFOUTSTAGE is not set}/$clustername/$(printf %02d $((${PROCESS:-0}/1000)))/$(printf %05d ${PROCESS:-0})"

#================================================================
# Set up Mu2e environment and make ifdh available
if source "${MU2EGRID_MU2ESETUP:?Error: MU2EGRID_MU2ESETUP: not defined}"; then

    setup -B ifdhc $IFDH_VERSION

    if type ifdh 2> $errfile; then

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

            ( mu2eprodsys_payload ) 3>&1 4>&2 1>> $logFileName 2>&1

            ret=$?

            shopt -u failglob
            shopt -s nullglob
            outfiles=( $logFileName *.art *.root *.json )
            if [[ "x$MU2EGRID_TRANSFER_ALL" == "x1" ]]; then
                outfiles=( $(filterOutProxy $(selectFiles *) ) )
            fi
        fi

        # Transfer the results.  There were cases when jobs failed after
        # creating the outstage directory, and were automatically restarted by
        # condor.  I also observed cases when more than one instance of the
        # same job, duplicated by some glitches in the grid system, completed
        # and transferred files back.  To prevent data corruption we write to
        # a unique tmp dir, than rename it to the final name.

        tmpOutDir="${finalOutDir}.$(od -A n -N 4 -t x4 /dev/urandom|sed -e 's/ //g')"


        t1=$(date +%s)
        # the -cd option causes gridftp to create all required directories in the output  path
        IFDH_GRIDFTP_EXTRA='-cd' ifdh cp --force=expftp -D "${outfiles[@]}" "${tmpOutDir}"
        ifdhret=$?

        if [[ $ifdhret -ne 0 ]]; then
            echo "The command: IFDH_GRIDFTP_EXTRA='-cd' ifdh cp --force=expftp -D ${outfiles[@]} ${tmpOutDir}" >&2
            echo "has failed on $(date) with status code $ifdhret.  Re-running with IFDH_DEBUG=10." >&2
            IFDH_DEBUG=10 IFDH_GRIDFTP_EXTRA='-cd' ifdh cp --force=expftp -D "${outfiles[@]}" "${tmpOutDir}" >&2
            ifdhret=$?
        fi

        if [[ ( $ret -eq 0 ) && ( $ifdhret -ne 0 ) ]]; then
            echo "ifdh cp failed on $(date): exit code $ifdhret" >&2
            ret=23
        fi

        if [[ $ifdhret -eq 0 ]]; then
            # ignore exit codes here - we've got the files
            ifdh rename "${tmpOutDir}" "${finalOutDir}" --force=expftp

            t2=$(date +%s)
            echo "$(date) # Total outstage time: $((t2-t1)) seconds"

            for i in "${outfiles[@]}"; do
                ifdh pin $f $((3600*24*7)) #  pin for a week
            done

            t3=$(date +%s)
            echo "$(date) # Total outstage pin time: $((t3-t2)) seconds"
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
