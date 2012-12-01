#!/bin/bash
#
# Grid worker node scrip for mu2e MARS jobs.  It is not meant to be
# invoked by users directly.  One should use mu2esub to provide proper
# environment for this script.
#
# Andrei Gaponenko, 2012
#

source "$(dirname $0)/funcs"

#================================================================
addMARSSeeds() {
    master=${1:?addMARSSeeds: arg1 missing}
    seed=${2:?addMARSSeeds:: arg2 missing}
    sed -e 's/STOP/SEED '$seed'\nSTOP/' $master > MARS.INP
}

#================================================================
createMARSOutStage() {
    # Copy arguments into meaningful names.
    outstagebase=${1:?createOutStage: outstagebase missing}
    user=${2:?createOutStage: user missing}
    fmt=${3:?createOutStage: jobname missing}
    cluster=${4:?createOutStage: cluster missing}
    process=${5:?createOutStage: process missing}

    outstage="${outstagebase}/$user/$(printf $fmt $cluster $process)"

    mkdir -p --mode 0775 "${outstage}" && echo "${outstage}"
}
#================================================================
umask 002

export startdir=$(pwd)
export cluster=${CLUSTER:-1}
export process=${PROCESS:-0}
export user=${MU2EGRID_SUBMITTER:?"Error: MU2EGRID_SUBMITTER not set"}
export executable=${MU2EGRID_EXECUTABLE:?"Error: MU2EGRID_EXECUTABLE not set"}
export masterinput=${MU2EGRID_MASTERINPUT:?"Error: MU2EGRID_MASTERINPUT not set"}
export outdirfmt=${MU2EGRID_OUTDIRFMT:?"Error: MU2EGRID_OUTDIRFMT not set"}
export topdir=${MU2EGRID_TOPDIR:?"Error: MU2EGRID_TOPDIR not set"}
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
SEED="${MU2EGRID_BASE_SEED:-$(generateSeed)}"
addMARSSeeds $masterinput "$SEED"

# Run the job
echo "Starting on host $(uname -a) on $(date)" >> mars.log 2>&1
echo "Running the command: $executable" >> mars.log 2>&1
echo "mu2egrid random seed $SEED" >> mars.log 2>&1
/usr/bin/time $executable >> mars.log 2>&1
ret=$?
echo "mu2egrid exit status $ret" >> mars.log 2>&1

# Transfer results
outdir="$(createMARSOutStage ${outstagebase} ${user} ${outdirfmt} ${cluster} ${process})"
transferOutFiles "$outdir" $(filterOutProxy *)

exit $ret