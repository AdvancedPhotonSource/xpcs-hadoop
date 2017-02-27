#!/bin/bash

export XPCS_HADOOP_DIR=.

params="$params -D hadoop.root.logger=DEBUG"
params="$params -D mapred.map.tasks=30 "
params="$params -D mapred.reduce.tasks=20 "
params="$params -D mapred.textoutputformat.separator=, "
params="$params -D mapred.compress.map.output=true "
params="$params -D mapred.compress.output=true "
params="$params -D xpcs.writeDarkImages=true "

mainClass="gov.anl.aps.xpcs.main.Application"

### MAGELLAN Parameters
endpoint='/xpcs'
hdf5_file='.'
mageelan=0
src_pattern='/net/wolf/data/xpcs8/'
dst_pattern='/mnt/data/xpcs/'

while getopts i:te:dm param
  do 
    case $param in
        i) 
            params=" $params -D xpcs.config.hdf5=$OPTARG "
            hdf5_file=$OPTARG
            ;;
        t) params=" $params -D xpcs.config.analysis_type=2";;
        e) 
            params=" $params -D xpcs.config.hdf5.endpoint=$OPTARG"
            endpoint=$OPTARG
            ;;
	    d) mainClass="gov.anl.aps.xpcs.main.DebugJob";; 
        m) magellan=1 ;;
    esac
  done

if [ $magellan -eq 1 ]; then
    path=`./magellan_path.py $endpoint $src_pattern $dst_pattern $hdf5_file`
    ./hadoop_copy.sh $path
fi

jhdf5_folder="$XPCS_HADOOP_DIR/lib/linux/x86_64"

export JAVA_LIBRARY_PATH=$jhdf5_folder:$JAVA_LIBRARY_PATH

export HADOOP_OPTS="-Djava.library.path=$JAVA_LIBRARY_PATH -Xmx1024m -Dlog4j.configuration=file:$XPCS_HADOOP_DIR/log4j.properties"

[ -f clean.sh ] && ./clean.sh

version='0.5.0'

hadoop_job="$XPCS_HADOOP_DIR/xpcs-hadoop-$version-all.jar"

# The pipeline process doesn't handle a large information (if thrown by hadoop process)
# on the standard out or standard error. For now, dump all that to /dev/nul
#hadoop jar $hadoop_job $mainClass $params 2>/dev/null

# Uncomment the line below and comment the one above for better debugging.
# hadoop jar $hadoop_job $mainClass $params

ecode=$?

exit $ecode
