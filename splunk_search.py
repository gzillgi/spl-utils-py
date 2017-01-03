__author__ = 'Greg Zillgitt'

#############################################################################
#
# Name: splunk_search
# Version: 1.1
# Description:
# Run abritrary splunk search and generate structured output
#
# Adapted from Splunk SDK examples
#
#
# .splunkrc file contains login and password;
#    this file needs to be in the user's home directory
#
# Example .splunkrc file contents:
#
#     # Splunk host (default: localhost) - search head
#     host=apsrp2256
#     # Splunk admin port (default: 8089)
#     port=8089
#     # Splunk username
#     username=myusername
#     # Splunk password
#     password=splunk
#     # Access scheme (default: https)
#     scheme=https
#     # Your version of Splunk (default: 5.0)
#     version=6.0
#
# Change log
# 2.0  07/20/16 GRZ initial version
# 2.1  07/29/16 GRZ added option to override host
#                   added option to override app context
#                   enhanced logging
#                   enhanced exception handling
# 2.2  09/09/16 GRZ handle multi-line search file
#                   allow for comments in search file
#
#############################################################################

# # # # # # # # # # # # # # # # # # # # # # # # # #
# Imports
# # # # # # # # # # # # # # # # # # # # # # # # # #

# installation support files
import platform
import sys
import time
from time import sleep
import datetime
import logging

# splunk support files
from splunklib.client import connect


try:
    from utils import parse
except ImportError:
    raise Exception("This script needs the SDK repository in PYTHONPATH "
                    "(e.g., export PYTHONPATH=~/splunk-sdk-python.")

# # # # # # # # # # # # # # # # # # # # # # # # # #
# globals
# # # # # # # # # # # # # # # # # # # # # # # # # #
version = "2.2"

OUTPUT_MODES = ["csv", "xml", "json"]
OUTPUT_MODE = "csv"
EARLIEST = "-24h"
LATEST = "now"
HOST_NAME = platform.uname()[1]
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%3N%z"
OUTFILE_PATH = "./output.dat"
LOGFILE_PATH = ""
APP_CONTEXT = "None"
DEQUOTE = "N"
ADDLF = "false"
KEEP_METADATA = "false"
LOG_LEVEL = "INFO"
JSON_CALLS = 0

CLIRULES = {
    'addlf': {
        'flags': ["--addlf"],
        'default': ADDLF,
        'help': "add extra lines to json output - default is %s" % ADDLF
    },
    'app': {
        'flags': ["--app"],
        'default': APP_CONTEXT,
        'help': "app context - default is %s" % APP_CONTEXT
    },
    'dequote': {
        'flags': ["--dequote"],
        'default': "N",
        'help': "Strip double quotes from output - default = keep double quotes"
    },
    'earliest': {
        'flags': ["--earliest"],
        'default': EARLIEST,
        'help': "earliest result time - default is %s" % EARLIEST
    },
    'host': {
        'flags': ["--host"],
        'help': "Splunk host - default from .splunkrc"
    },
    'username': {
        'flags': ["--username"],
        'help': "Splunk username - default from .splunkrc"
    },
    'password': {
        'flags': ["--password"],
        'help': "Splunk password - default from .splunkrc"
    },
    'keepmetadata': {
        'flags': ["--keepmetadata"],
        'default': "false",
        'help': "Prevent stripping of metadata from output - default = false (strip metadata)"
    },
    'latest': {
        'flags': ["--latest"],
        'default': LATEST,
        'help': "latest result time - default is %s" % LATEST
    },
    'logfile': {
        'flags': ["--logfile"],
        'default': LOGFILE_PATH,
        'help': "log file path - default = no log file"
    },
    'loglevel': {
        'flags': ["--loglevel"],
        'default': LOG_LEVEL,
        'help': "Log level - default is %s" % LOG_LEVEL
    },
    'omode': {
        'flags': ["--omode"],
        'default': OUTPUT_MODE,
        'help': "output format %s default is %s" % (OUTPUT_MODES, OUTPUT_MODE)
    },
    'outfile': {
        'flags': ["--outfile"],
        'default': OUTFILE_PATH,
        'help': "output file path - default is %s" % OUTFILE_PATH
    },
    'search': {
        'flags': ["--search"],
        'default': "search INDEX=NULLSEARCH",
        'help': "search string (default 'search INDEX=NULLSEARCH')"
    },
    'searchfile': {
        'flags': ["--searchfile"],
        'default': "",
        'help': "File containing search string - default = no file"
    },
    'timeformat': {
        'flags': ["--timeformat"],
        'default': TIME_FORMAT,
        'help': "_time output format - default is %s" % LATEST
    }
}

# parse command line options
OPTIONS = parse(sys.argv[1:], CLIRULES, ".splunkrc")

# # # # # # # # # # # # # # # # # # # # # # # # # #
# Set up logging
# # # # # # # # # # # # # # # # # # # # # # # # # #
# LOG_LEVEL = logging.DEBUG
OPT_LOGLEVEL = OPTIONS.kwargs['loglevel'].upper()
if OPT_LOGLEVEL == "INFO":
    level = logging.INFO
elif OPT_LOGLEVEL == "WARNING":
    level = logging.WARNING
elif OPT_LOGLEVEL == "ERROR":
    level = logging.ERROR
elif OPT_LOGLEVEL == "DEBUG":
    level = logging.DEBUG
else:
    level = logging.INFO

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=level, format=LOG_FORMAT)
logger = logging.getLogger()

# # # # # # # # # # # # # # # # # # # # # # # # # #
# Fetch command line options
# # # # # # # # # # # # # # # # # # # # # # # # # #
OPT_OMODE = OPTIONS.kwargs['omode']
OPT_SEARCH = OPTIONS.kwargs['search']
OPT_EARLIEST = OPTIONS.kwargs['earliest']
OPT_LATEST = OPTIONS.kwargs['latest']
OPT_TIME_FORMAT = OPTIONS.kwargs['timeformat']
OPT_OUTFILE = OPTIONS.kwargs['outfile']
OPT_LOGFILE = OPTIONS.kwargs['logfile']
OPT_HOST = OPTIONS.kwargs['host']
OPT_APP_CONTEXT = OPTIONS.kwargs['app']
OPT_DEQUOTE = OPTIONS.kwargs['dequote']
OPT_SEARCHFILE = OPTIONS.kwargs['searchfile']
OPT_KEEP_METADATA = OPTIONS.kwargs['keepmetadata']
OPT_ADDLF = OPTIONS.kwargs['addlf']
OPT_USERNAME = OPTIONS.kwargs['username']
OPT_PASSWORD = OPTIONS.kwargs['password']

# # # # # # # # # # # # # # # # # # # # # # # # # #
# Add flat file appender if specified
# # # # # # # # # # # # # # # # # # # # # # # # # #
if OPT_LOGFILE != "":
    fh = logging.FileHandler(OPT_LOGFILE)
    fmt = logging.Formatter(fmt=LOG_FORMAT)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

# # # # # # # # # # # # # # # # # # # # # # # # # #
# Read file containing splunmk search query
# # # # # # # # # # # # # # # # # # # # # # # # # #
if OPT_SEARCHFILE != "":
    OPT_SEARCH = ""
    try:
        sf = open(OPT_SEARCHFILE, 'r')
        for line in sf:
            if (len(line) > 0) and (line[0] != "#"):
                OPT_SEARCH += line.rstrip('\n')

    except IOError:
        logger.error("Failed to open search file " + OPT_SEARCHFILE)
        sys.exit(1)

if len(OPT_SEARCH) == 0:
    print "required parameter '--search' missing or empty"
    sys.exit(2)

#
# - By default, Splunk will return metadata fields such as _time and host
# - To prevent this set OPT_KEEP_METADATA=FALSE
# - Important: the macro "strip_metadata" must be accessible from the user's
#   Splunk app context
#
#   As of 9/12/16 the macro looks like this:
#
#   |fields - _*, "date_hour", "date_mday", "date_minute", "date_month", "date_second", "date_wday",
#   "date_year","date_zone", dttm, eventtype, host, id, index, linecount, punct, source, sourcetype,
#   "splunk_server", "splunk_server_group", tag, "tag::eventtype", timeendpos, timestartpos, "virtual_server"
#
if OPT_KEEP_METADATA == "false":
    if OPT_SEARCH.find('`strip_metadata`') < 0: # check if search already references this macro
        logger.debug("Adding `strip_metadata` to end of search string")
        OPT_SEARCH = OPT_SEARCH + " `strip_metadata`"

OPTIONS.kwargs['search'] = OPT_SEARCH

# # # # # # # # # # # # # # # # # # # # # # # # # #
# Open output file
# # # # # # # # # # # # # # # # # # # # # # # # # #
try:
    logger.debug("Opening output file: " + OPT_OUTFILE)
    of = open(OPT_OUTFILE, 'w')
except IOError:
    logger.error("Failed to open output file " + OPT_OUTFILE)
    sys.exit(1)


def write_csv_event(content, start):
    of.writelines(content)


def write_json_event(content, start):
    global JSON_CALLS
    JSON_CALLS += 1
    if OPT_ADDLF.lower() == "true":
        of.writelines(content[0] + content[1:].replace("{", "\n{"))
    else:
        of.writelines(content)


def write_xml_event(content, start):
    of.writelines(content)


def write_event(content, start):
    if OPT_OMODE == "csv":
        write_csv_event(content, start)
    elif OPT_OMODE == "json":
        write_json_event(content, start)
    else:
        write_xml_event(content, start)


def run_search_normal(service):
    squery = OPT_SEARCH

    # add "search" keyword to the beginning of the query if not present
    if not OPT_SEARCH.lower().startswith("search "):
        squery = "search " + squery

    kwargs_search = {"earliest_time": OPT_EARLIEST,
                     "latest_time": OPT_LATEST,
                     "count": 0}
    logger.debug(kwargs_search)
    logger.info("calling Splunk API...")
    job = service.jobs.create(squery, **kwargs_search)

    while True:

        # wait until search results are ready
        while not job.is_ready():
            pass

        stats = {"isDone": job["isDone"],
                 "doneProgress": float(job["doneProgress"]) * 100,
                 "scanCount": int(job["scanCount"]),
                 "eventCount": int(job["eventCount"]),
                 "resultCount": int(job["resultCount"])}

        status = ("\r%(doneProgress)03.1f%%   %(scanCount)d scanned   "
                  "%(eventCount)d matched   %(resultCount)d results") % stats

        logger.debug(status)

        if stats["isDone"] == "1":
            logger.debug("Fetched all results")
            break

        # TODO: experiment with shorter sleep periods
        sleep(1)

    # print "======================================================="
    kwargs_results = {"output_mode": OPT_OMODE, "f": ["*"], "count": 0}
    logger.debug(kwargs_results)
    line_count = 0
    byte_count = 0
    start = True
    for result in job.results(**kwargs_results):
        line_count += result.count("\n")
        byte_count += len(result)
        if start:
            write_event(result, True)
            start = False
        else:
            write_event(result, False)

    of.flush()

    job.cancel()
    # sys.stdout.write('\n')
    line_count += 1

    logger.info("search complete; " + str(byte_count) + " bytes and " + str(
        line_count) + " physical lines output to " + of.name)


# end run_search_normal()


def run_search(service):
    if len(OPT_SEARCH) == 0:
        print "required parameter '--search' missing or empty"
        sys.exit(2)

    squery = OPT_SEARCH

    # add "search" keyword if not present
    if not OPT_SEARCH.lower().startswith("search "):
        squery = "search " + squery

    success = False
    result = None

    while not success:
        #
        # issue query to splunkd
        # count=0 overrides the maximum number of events
        # returned (normally 50K)
        #
        # TODO: Catch socket exceptions and retry (optionally)
        #
        logger.info("Calling Splunk API; search=" + squery)
        try:
            result = service.get('search/jobs/export',
                                 search=squery,
                                 sort_dir="desc",
                                 output_mode=OPT_OMODE,
                                 timeout=120,
                                 earliest_time=OPT_EARLIEST,
                                 latest_time=OPT_LATEST,
                                 f=["*"],
                                 count=0)
        except Exception as e:
            logger.error("Exception calling service.get to Splunk: " + str(e))
            sys.exit(2)

        if result.status != 200:
            logger.warn("warning: export job failed: %d, sleep/retry" % result.status)
            time.sleep(60)
        else:
            logger.debug("called service.get()")
            success = True

    line_count = 0
    while True:
        content = result.body.read()
        chunk_length = len(content)
        if chunk_length == 0:
            break
        logger.debug("Got chunk - " + str(chunk_length) + " bytes")
        line_count += content.count("\n", 0, chunk_length)

        if OPT_DEQUOTE == "Y":
            of.writelines(content.replace('"', ''))
        else:
            of.writelines(content)

    logger.info("run_search complete; " + str(line_count) + " lines output to " + of.name)


# end run_search()

def main():
    # Remove app context if not provided
    if OPT_APP_CONTEXT == "" or OPT_APP_CONTEXT == "None":
        OPTIONS.kwargs.pop('app')
    else:
        logger.debug("setting app context to " + OPTIONS.kwargs['app'])

    # Connect to Splunk
    logger.info("Connecting to splunk server: " + OPTIONS.kwargs['host'] + " ...")
    logger.debug(OPTIONS.kwargs)
    try:
        service = connect(**OPTIONS.kwargs)
    except Exception as e:
        logger.error("Exception connecting to Splunk: " + str(e))
        sys.exit(2)

    logger.debug("... connected.")

    #   Run the Splunk search
    logger.debug("Calling run_search_normal()...")
    search_start = time.time()
    run_search_normal(service)
    logger.debug("... returned from run_search_normal().")
    search_end = time.time()
    search_delta = search_end - search_start
    logger.info("started: " + str(datetime.datetime.fromtimestamp(search_start))
                + "; ended: " + str(datetime.datetime.fromtimestamp(search_end))
                + "; runtime: " + str(datetime.timedelta(seconds=search_delta)))
    if OPT_OMODE == "json":
        logger.debug(str(JSON_CALLS) + " json calls")

    # flush output buffer
    of.flush()
    of.close()

    logger.info("Splunk search script complete")


if __name__ == '__main__':
    main()
