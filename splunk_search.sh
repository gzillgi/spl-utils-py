#!/usr/bin/env bash

#
# wrapper script for splunk_search.py
#
# Change Log
#
# 09/08/16       GRZ     Initial release
#

export SCRIPTDIR=`dirname $0`
export SCRIPTNAME=`basename $0`
export SDKDIR=~/bin/splunk-sdk-python-1.6.0
export SPLUNK_HOME=/opt/splunk/splunkforwarder
export PYTHONPATH=$SPLUNK_HOME/lib/python2.7:$SDKDIR
export LOGDIR=$SCRIPTDIR/log
export LOGFNAME=l7_extract.log
export LOGPATH=$LOGDIR/$LOGFNAME
export PIDPATH=$SCRIPTDIR/splunk_search.pid
#export PYSCRIPTPATH=$SCRIPTDIR/l7_extract.py
export PYSCRIPTPATH=$SCRIPTDIR/splunk_search.2.1.py
export RECIPIENTS="greg_zillgitt@optum.com"
export PWDFILE=$SCRIPTDIR/.splnkfwd.pwd

# set next run to 10 minutes past the next hour
export EPOCH=`date +%s`
export NEXTRUN=`date '+%y%m%d%H' --date="@$((EPOCH + 3600))"`
export NEXTRUN=$NEXTRUN"10"
export EVENTHR=`date '+%y%m%d%H' --date="@$((EPOCH - 7772400))"`

export OUTDIR=/datalake/corporate/api_gateway_logs/L7_logs
export OUTFNAME=layer7_splunk_archive_`date +"%Y%m%d%H%M"`_$EVENTHR.raw
export OUTPATH=$OUTDIR/$OUTFNAME
export TMPDIR=$SCRIPTDIR
export TMPPATH=$TMPDIR/$OUTFNAME

send_email() {
	echo "$1" | mail -s "splunk_search notice" $RECIPIENTS
}

log() {
	echo "`date -Iseconds` $0 - $1" >> $LOGPATH
}

log "started;  OUTPATH=$OUTPATH"

# check for PID file - don't run if it exists
L7_EXTRACT_PID=`cat $PIDPATH`
if [ "$L7_EXTRACT_PID" == "" ]; then
	export L7_EXTRACT_PID=$$
	log "writing PID ($L7_EXTRACT_PID) to $PIDPATH"
	echo $$ > $PIDPATH
else
	msg="ERROR: $PIDPATH  exists - possible crash; PID=$L7_EXTRACT_PID"
	log "$msg"
	send_email "$msg"
	exit 1
fi

# Create a MapR authentication ticket
pw=`cat $PWDFILE`
printf "${pw}\n" | maprlogin password 2>&1 >> $LOGPATH
log "`maprlogin print`"

# run Splunk search output to CSV
RETRIES=5
ATTEMPT=0
SLEEP=60
SLEEPADD=300
while [ $ATTEMPT -le $RETRIES ]; do
	python $PYSCRIPTPATH --earliest -90d@h --latest -90d@h+1h --search "index=layer7 | table _raw" --outfile $TMPPATH.tmp --logfile $LOGPATH
	PYTHON_RC=$?
	log "$PYSCRIPTPATH finished;  rc=$PYTHON_RC"

	if [ $PYTHON_RC -eq 0 ]; then
		mv $TMPPATH.tmp $TMPPATH
		log "wrote `cat $TMPPATH | wc -l -c` events/bytes"
		log "started moving $TMPPATH to $OUTPATH ..."
		hadoop fs -moveFromLocal $TMPPATH $OUTPATH
		log "... finished moving"
		# delete PID file if successful
		rm $PIDPATH
		log "next scheduled run at $NEXTRUN"
		echo "$0" | at -t $NEXTRUN
		break
	else
		msg="$0 ERROR: $PYSCRIPTPATH returned rc=$PYTHON_RC; PID=$L7_EXTRACT_PID; `wc -c < $TMPPATH.tmp` bytes written; ATTEMPT=$ATTEMPT"
		log "$msg"
		send_email "$msg"
	fi
	let ATTEMPT=ATTEMPT+1
	sleep $SLEEP
	let SLEEP=SLEEP+SLEEPADD
done

