#!/bin/bash
#
# Grid worker node scrip for mu2e framework jobs.  It is not meant to
# be invoked by users directly.  One should use the mu2eprodsys user
# interface program to prepare the environment and submit this script.
#
# Andrei Gaponenko, 2014, 2015
#

#================================================================
printinfo() {
    echo Starting on host `uname -a` on `date`
    echo running as user `id`
    echo "current work dir is $(/bin/pwd)"
    echo OS version `cat /etc/redhat-release`
    echo "job arguments: $@"
    echo "#================================================================"
    echo "Visible disk space:"
    df -P
    echo "#================================================================"
    echo "TMPDIR: ls -alR"
    ls -alR "$TMPDIR"
    echo "TMPDIR: df -h"
    df -h "$TMPDIR"
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
ifdh_mkdir_p() {
    local dir="$1"
    local force="$2"

    #### "ifdh ls" exits with 0 even for non-existing dirs.
    ## if ifdh ls $dir 0 $force > /dev/null
    if [ $(ifdh ls $dir 0 $force  2>/dev/null |wc -l) -gt 0 ]
    then
        : # done
    else
        if [ x"$dir" == x/ ]; then # protection against an infinite loop
            echo "ifdh_mkdir_p: error from ifdh ls / 0" >&2
            exit 1
        fi

        ifdh_mkdir_p $(dirname $dir) $force
        ifdh mkdir $dir $force
        ifdh chmod 0755 $dir $force

    fi
}
#================================================================
# Run the framework jobs and create json files for the outputs
# Running it inside a function makes it easier to exit on error
# during the "payload" part, but still transfer the log file back.
mu2eprodsys_payload() {

    mu2epseh() {
        echo "Error from $BASH_COMMAND: exit code $?"
        exit $?
    }
    trap mu2epseh ERR

    echo "Copying in $origFCL"
    ifdh cp $origFCL $localFCL

    #================================================================

    if source "${MU2EGRID_USERSETUP:?Error: MU2EGRID_USERSETUP: not defined}"; then

        setup mu2ebintools -q "${MU2E_UPS_QUALIFIERS}"
        setup sam_web_client

        echo "#================================================================"
        echo "After package setup, the environment is:"
        /usr/bin/printenv
        echo "#================================================================"

        timecmd=time  # shell builtin is the fallback option
        if [ -x /usr/bin/time ]; then
            # our first choice: GNU time provided by the system
            timecmd=/usr/bin/time
        else
            # FIXME: package time as a UPS product.
            #
            # There is a copy of GNU time on CVMFS, but not as a UPS
            # package.  We'd need to re-implement a part of UPS to
            # select the correct binary to run on the current node.
            # Instead just try to run the SL6 version and use it if
            # successful.
            mu2etime=/cvmfs/mu2e.opensciencegrid.org/bin/SLF6/time
            if $mu2etime true > /dev/null 2>&1; then timecmd=$mu2etime; fi
        fi

        #================================================================
        # Pre-stage input data files, and write their SAM names
        # to the "parents" file for later use

        mkdir mu2egridInDir
        touch localFileDefs
        for key in $(fhicl-getpar --strlist mu2emetadata.fcl.inkeys $localFCL); do

            for rfn in $(fhicl-getpar --strlist $key $localFCL ); do
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

        if [[ -e prestage_spec ]]; then
            echo "$(date) # Starting to pre-stage input files"
            type ifdh
            tstart=$(date +%s)
            ifdh cp -f prestage_spec
            t2=$(date +%s)
            echo "$(date) # Total stage-in time: $((t2-tstart)) seconds, status $ret"

            echo "#----------------------------------------------------------------" >> $localFCL
            echo "# code added by mu2eprodys" >> $localFCL

            # set input file names
            cat localFileDefs >> $localFCL
        fi

        # set output file names

        for key in $(fhicl-getpar --strlist mu2emetadata.fcl.outkeys $localFCL ); do
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

        for i in *.art; do
            ${MU2E_BASE_RELEASE}/Tools/DH/jsonMaker.py \
                -f ${ffprefix}-sim \
                -a parents \
                -i mc.generator_type=$(fhicl-getpar --string mu2emetadata.mc.generator_type $localFCL) \
                -i mc.simulation_stage=$(fhicl-getpar --int    mu2emetadata.mc.simulation_stage $localFCL) \
                -i mc.primary_particle=$(fhicl-getpar --string mu2emetadata.mc.primary_particle $localFCL) \
                -x \
                $i
        done

        for i in *.root; do
            ${MU2E_BASE_RELEASE}/Tools/DH/jsonMaker.py \
                -f ${ffprefix}-nts \
                -a parents \
                -i mc.generator_type=$(fhicl-getpar --string mu2emetadata.mc.generator_type $localFCL) \
                -i mc.simulation_stage=$(fhicl-getpar --int    mu2emetadata.mc.simulation_stage $localFCL) \
                -i mc.primary_particle=$(fhicl-getpar --string mu2emetadata.mc.primary_particle $localFCL) \
                -x \
                $i
        done

        declare -a manifestfiles=( *.art *.root *.json )

        # A file should be immutable after its json is created.
        # addManifest appends to the log file; log.json has to be made after that.
        addManifest $logFileName "${manifestfiles[@]}" >&3 2>&4

        for i in $logFileName; do
            ${MU2E_BASE_RELEASE}/Tools/DH/jsonMaker.py \
                -f ${ffprefix}-etc \
                -a parents \
                -i mc.generator_type=$(fhicl-getpar --string mu2emetadata.mc.generator_type $localFCL) \
                -i mc.simulation_stage=$(fhicl-getpar --int    mu2emetadata.mc.simulation_stage $localFCL) \
                -i mc.primary_particle=$(fhicl-getpar --string mu2emetadata.mc.primary_particle $localFCL) \
                -x \
                $i >&3 2>&4
        done

    else
        echo "Error sourcing setup script ${MU2EGRID_USERSETUP}: status code $?"
    fi
}
export mu2eprodsys_payload

#================================================================
# Execution starts here

umask 002

# TMPDIR is defined and created by Condor.
cd $TMPDIR

# make sure we are not stuck with stale CVMFS data
CVMFSHACK=/cvmfs/grid.cern.ch/util/cvmfs-uptodate
test -x $CVMFSHACK && $CVMFSHACK /cvmfs/mu2e.opensciencegrid.org

ret=1
cluster=$(printf %06d ${CLUSTER:-0})

jobname=failedjob
export logFileName="${jobname}.log"
declare -a outfiles=( $logFileName )
finalOutDir="/pnfs/mu2e/scratch/outstage/${MU2EGRID_SUBMITTER:?Error: MU2EGRID_SUBMITTER is not set}/$cluster/$jobname.${PROCESS}"

#================================================================
# Set up Mu2e environment and make ifdh available
if source "${MU2EGRID_MU2ESETUP:?Error: MU2EGRID_MU2ESETUP: not defined}"; then

    setup ifdhc $IFDH_VERSION

    printinfo >> $logFileName 2>&1

    masterlist="$CONDOR_DIR_INPUT/${MU2EGRID_INPUTLIST:?MU2EGRID_INPUTLIST environment variable is not set}";
    export origFCL=$(getFCLFileName $masterlist ${PROCESS:?PROCESS environment variable is not set}) 2>> $logFileName

    if [ -n "$origFCL" ]; then

        # set current user and version info to obtain the name of this job
        jobname=$(basename $origFCL .fcl | awk -F . '{OFS="."; $2="'${MU2EGRID_DSOWNER:?"Error: MU2EGRID_DSOWNER is not set"}'"; $4="'${MU2EGRID_DSCONF}'"; print $0;}')
        newLogFileName=$(echo $jobname|awk -F . '{OFS="."; $1="log"; print $0;}').log
        mv "$logFileName" "$newLogFileName"
        export logFileName=$newLogFileName

        export localFCL="./$jobname.fcl"

        finalOutDir="/pnfs/mu2e/scratch/outstage/${MU2EGRID_SUBMITTER:?Error: MU2EGRID_SUBMITTER is not set}/$cluster/$jobname"

        ( mu2eprodsys_payload ) 3>&1 4>&2 1>> $logFileName 2>&1

        ret=$?

        outfiles=( $logFileName *.art *.root *.json )
    fi

    # Transfer the results.  There were cases when jobs failed after
    # creating the outstage directory, and were automatically restarted by
    # condor.  I also observed cases when more than one instance of the
    # same job, duplicated by some glitches in the grid system, completed
    # and transferred files back.  To prevent data corruption we write to
    # a unique tmp dir, than rename it to the final name.

    # Create the "cluster level" output directory.
    ifdh_mkdir_p "$(dirname ${finalOutDir})" --force=expftp

    # There is no "mktemp" in ifdh.  Imitate it by hand
    tmpOutDir=''
    numTries=0
    while [ $((numTries+=1)) -le 5 ]; do
        tmpOutDir="${finalOutDir}.$(od -A n -N 4 -t x4 /dev/urandom|sed -e 's/ //g')"

        if ifdh mkdir "$tmpOutDir" --force=expftp ; then break; fi

        echo "Attemtp to make tmpOutDir = $tmpOutDir failed"
        tmpOutDir=''
    done

    if [ x"$tmpOutDir" != x ]; then

        ifdh chmod 0755 "${tmpOutDir}" --force=expftp

        t1=$(date +%s)

        ifdh cp --force=expftp -D "${outfiles[@]}" "${tmpOutDir}"  || ret=2
        ifdh rename "${tmpOutDir}" "${finalOutDir}" --force=expftp

        t2=$(date +%s)
        echo "$(date) # Total outstage lock and copy time: $((t2-t1)) seconds"

        for i in "${outfiles[@]}"; do
            ifdh pin $f $((3600*24*7)) #  pin for a week
        done

        t3=$(date +%s)
        echo "$(date) # Total outstage pin time: $((t3-t2)) seconds"
    fi

else
    echo "Error sourcing setup script ${MU2EGRID_MU2ESETUP}: status code $?"
fi

exit $ret
