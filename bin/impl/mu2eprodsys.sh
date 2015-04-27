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
    echo "================================================================"
    echo Starting on host `uname -a` on `date`
    echo running as user `id`
    echo "current work dir is $(/bin/pwd)"
    echo OS version `cat /etc/redhat-release`
    echo "job arguments: $@"
    echo "The environment is:"
    /usr/bin/printenv
    echo "================================================================"
    echo "Visible disk space:"
    df -P
    echo "================================================================"
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
    echo "# selfcheck: $sc" >> $manifest
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
        exit 1
    }
    trap mu2epseh ERR

    ifdh cp $origFCL $localFCL

    sed -e "s/MU2EGRIDDSOWNER/$MU2EGRID_DSOWNER/g" -e "s/MU2EGRIDDSCONF/$MU2EGRID_DSCONF/g" $localFCL > $localFCL.tmp

    mv $localFCL.tmp $localFCL
    # include the edited copy of the fcl into the log?

    # FIXME: pre-stage input data files - not needed for stage 1

    if source "${MU2EGRID_USERSETUP:?Error: MU2EGRID_USERSETUP: not defined}"; then

        setup mu2ebintools -q "${MU2E_UPS_QUALIFIERS}"

        printinfo

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

        # Run the job
        echo "#================================================================"
        echo "Running the command: $timecmd mu2e -c $localFCL"
        $timecmd mu2e -c $localFCL
        ret=$?

        echo "mu2egrid exit status $ret"

        echo "#================================================================"

        # Create SAM metadata for the outputs.

        case ${MU2EGRID_DSOWNER} in
            mu2e*) ffprefix=phy ;;
            *)     ffprefix=usr ;;
        esac

        # FIXME: add  all parents using the pre-stage information
        echo $(basename $origFCL) > parents

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

        declare -a outfiles=( *.art *.root *.json )

        # A file should be immutable after its json is created.
        # addManifest appends to the log file; log.json has to be made after that.
        addManifest $logFileName "${outfiles[@]}" >&3 2>&4

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
        ret=1
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

#================================================================
export origFCL=$(getFCLFileName "${MU2EGRID_INPUTLIST}" ${PROCESS:?PROCESS environment variable is not set})

# set current user and version info to obtain the name of this job
jobname=$(basename $origFCL .fcl | awk -F . '{OFS="."; $2="'${MU2EGRID_DSOWNER:?"Error: MU2EGRID_DSOWNER is not set"}'"; $4="'${MU2EGRID_DSCONF}'"; print $0;}')

export localFCL="./$jobname.fcl"
export logFileName="${jobname}.log"

cluster=$(printf %06d ${CLUSTER:-0})
finalOutDir="/pnfs/mu2e/scratch/outstage/${MU2EGRID_SUBMITTER:?Error: MU2EGRID_SUBMITTER is not set}/$cluster/$jobname"

ret=1

#================================================================
# Set up Mu2e environment and make ifdh available
if source "${MU2EGRID_MU2ESETUP:?Error: MU2EGRID_MU2ESETUP: not defined}"; then

    setup ifdhc $IFDH_VERSION

    ( mu2eprodsys_payload ) 3>&1 4>&2 1>> $logFileName 2>&1

    declare -a outfiles=( *.art *.root $logFileName *.json )

    # Transfer the results.  There were cases when jobs failed after
    # creating the outstage directory, and were automatically restarted by
    # condor.  I also observed cased when more than one instance of the
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

        ifdh cp --force=expftp -D "${outfiles[@]}" "${tmpOutDir}"
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
    ret=1
fi

exit $ret
