#!/bin/bash
#
# Grid worker node scrip for mu2e framework jobs.  It is not meant to be
# invoked by users directly.  One should use mu2esub to provide proper
# environment for this script.
#
# The copyback.sh script is responsible for transferring the outputs
# of a job from the worker node disk.
#
# Andrei Gaponenko, 2014
#

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
createManifest() {
    ls -l |awk '{print "# "$0}' > manifest
    echo '#----------------------------------------------------------------' >> manifest
    echo '# algorithm: sha256sum' >> manifest
    sha256sum "$@" >> manifest
    sc="$(sha256sum < manifest)"
    echo "# selfcheck: $sc" >> manifest
    echo manifest
}
#================================================================
transferOutFiles() {
    echo "$(date) # Starting to transfer output files"
    type ifdh

    OUTDIR="${1:?transferOutFiles: OUTDIR arg missing}"
    shift

    MANIFEST=$(createManifest "$@")

    t1=$(date +%s)

    ifdh cp --force=expftp -D  "$MANIFEST" "$@" ${OUTDIR}

    t2=$(date +%s)
    echo "$(date) # Total outstage lock and copy time: $((t2-t1)) seconds"
}
#================================================================

umask 002

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

user=${MU2EGRID_SUBMITTER:?"Error: MU2EGRID_SUBMITTER is not set"}
outfmt=${MU2EGRID_OUTDIRFMT:?"Error: MU2EGRID_OUTDIRFMT is not set"}
outstagebase=${MU2EGRID_OUTSTAGE:?"Error: MU2EGRID_OUTSTAGE is not set"}

# Run the job
"$(dirname $0)/${1:?Error: copyback.sh arg missing}" > mu2e.log 2>&1
ret=$?

# Transfer the results.  There were cases when jobs failed after
# creating the outstage directory, and were automatically restarted by
# condor.  I also observed cased when more than one instance of the
# same job, duplicated by some glitches in the grid system, completed
# and transferred files back.  To prevent data corruption we write to
# a unique tmp dir, than rename it to the final name.

finalOutDir="${outstagebase}/$user/$(printf $outfmt  ${CLUSTER:-1} ${PROCESS:-0})"
mkdir -p --mode 0775 "$(dirname ${finalOutDir})"
tmpOutDir="$(mktemp -d $finalOutDir.XXX)"
chmod 0775 "${tmpOutDir}"
transferOutFiles "${tmpOutDir}" $(filterOutProxy $(selectFiles *) )
/bin/mv "${tmpOutDir}" "${finalOutDir}"

exit $ret
