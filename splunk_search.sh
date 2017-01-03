#!/usr/bin/env bash

#
# wrapper script for splunk_search.py
#
# Change Log
#
# 09/08/16       GRZ     Initial release
#

export SCRIPTDIR=`dirname $0`
export PYSCRIPTPATH=$SCRIPTDIR/splunk_search.py
export SPLUNK_SEARCH_FILE=$SCRIPTDIR/splunk_search.txt

export LOGFNAME=splunk_search.log
export LOGDIR=$SCRIPTDIR/log
export LOGPATH=$LOGDIR/$LOGFNAME

export SDKDIR=~/bin/splunk-sdk-python-1.6.0
export PYTHONPATH=$SDKDIR

export OUTDIR=$SCRIPTDIR/out
export OUTFNAME=splunk_search.csv
export OUTPATH=$OUTDIR/$OUTFNAME
export TMPDIR=$SCRIPTDIR
export TMPPATH=$TMPDIR/$OUTFNAME

EARLIEST=`cat $STATE_FILE`
`"

# run Splunk search output to CSV

python $PYSCRIPTPATH --loglevel debug --earliest "-1d@d" --latest "now" \
    --searchfile $SPLUNK_SEARCH_FILE \
    --outfile $OUTPATH \
    --logfile $LOGPATH
