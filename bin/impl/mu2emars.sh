#!/bin/bash
#
# Grid worker node scrip for mu2e MARS jobs.  It is not meant to be
# invoked by users directly.  One should use mu2esub to provide proper
# environment for this script.
#
# Andrei Gaponenko, 2012
#

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
    seed=0
    maxseed=$((900000000))
    while [ "$seed" -le 0 ]; do 
	seed=$(( $(od --format u4 --read-bytes 4 /dev/urandom | head -1| awk '{print $2}') % maxseed ))
    done
    echo $seed
}

#================================================================
addMARSSeeds() {
    master=${1:?addMARSSeeds: arg1 missing}
    seed=${2:?addMARSSeeds:: arg2 missing}
    sed -e 's/STOP/SEED '$seed'\nSTOP/' $master > MARS.INP
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
    echo "Local disk space:"
    df -l -P
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
export executable=${MU2EGRID_EXECUTABLE:?"Error: MU2EGRID_EXECUTABLE not set"}
export masterinput=${MU2EGRID_MASTERINPUT:?"Error: MU2EGRID_MASTERINPUT not set"}
export jobname=${MU2EGRID_JOBNAME:?"Error: MU2EGRID_JOBNAME not set"}
export topdir=${MU2EGRID_TOPDIR:?"Error: MU2EGRID_TOPDIR not set"}

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
# Make input files accessible

ln -s $topdir/xsdir
ln -s $topdir/*.INP xsdir $WORKDIR
/bin/rm -f MARS.INP

# Create job config "MARS.INP" by adding random seeds to the master
addMARSSeeds $masterinput "${MU2EGRID_BASE_SEED:-$(generateSeed)}"

# Run the job
echo "Starting on host $(uname -a) on $(date)" >> testlog.log 2>&1
echo "Running the command: $executable" >> testlog.log 2>&1
/usr/bin/time $executable >> testlog.log 2>&1

# Transfer results
OUTDIR="$(createOutStage ${outstagebase} ${user} ${jobname} ${cluster} ${process})"

/grid/fermiapp/minos/scripts/lock 
for f in *; do
    case "$f" in
	*.proxy)
            # Don't expose security sensitive info
	    echo Skipping proxy file $f;;
	*) 
	    CMD="${GLOBUS_LOCATION}/bin/globus-url-copy -dbg -vb  file://${WORKDIR}/$f gsiftp://if-gridftp-marsmu2e.fnal.gov//${OUTDIR}/"
	    echo "about to do gridftp, the command is $CMD"
	    $CMD ;;
    esac
done
/grid/fermiapp/minos/scripts/lock free

exit 0
