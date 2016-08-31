#!/bin/bash

export XPCS_HADOOP_DIR=/local/xpcs-0.5.0

params="$params -D hadoop.root.logger=DEBUG"
params="$params -D norm.map.tasks=120 " 
params="$params -D norm.reduce.tasks=120 " 
params="$params -D mapred.map.tasks=30 "
params="$params -D mapred.reduce.tasks=20 "
params="$params -D mapred.textoutputformat.separator=, "
params="$params -D mapred.reduce.parallel.copies=20 "
params="$params -D fs.inmemory.size.mb=1024 "
params="$params -D io.sort.mb=1024 "
params="$params -D io.sort.factor=15 "
params="$params -D io.sort.record.percent=0.05 "
params="$params -D io.sort.spill.percent=0.95 "
params="$params -D mapred.child.java.opts=-Xmx2g "
params="$params -D mapred.job.map.memory.mb=1024 "
params="$params -D mapred.job.reduce.memory.mb=1024 "
params="$params -D mapred.cluster.map.memory.mb=500 "
params="$params -D mapred.cluster.max.map.memory.mb=1024 "
params="$params -D mapred.cluster.reduce.memory.mb=500 "
params="$params -D mapred.cluster.max.reduce.memory.mb=1024 "
params="$params -D mapred.reduce.tasks.speculative.execution=false "
params="$params -D mapred.job.reuse.jvm.num.tasks=-1 "
params="$params -D mapred.output.compression.type=BLOCK "
#params="$params -D mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec "
#params="$params -D mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec "
params="$params -D mapred.compress.map.output=true "
params="$params -D mapred.compress.output=true "
params="$params -D xpcs.writeDarkImages=true "

mainClass="gov.anl.aps.xpcs.main.Application"

### Parse script parameters
while getopts i:te:d param
  do 
    case $param in
        i) params=" $params -D xpcs.config.hdf5=$OPTARG ";;
        t) params=" $params -D xpcs.config.analysis_type=2";;
        e) params=" $params -D xpcs.config.hdf5.endpoint=$OPTARG";;
	    d) mainClass="gov.anl.aps.xpcs.main.DebugJob";; 
    esac
  done

jhdf5_folder="$XPCS_HADOOP_DIR/lib/linux"

export JAVA_LIBRARY_PATH=$jhdf5_folder:$JAVA_LIBRARY_PATH

export HADOOP_OPTS="-Djava.library.path=$JAVA_LIBRARY_PATH -Xmx1024m -Dlog4j.configuration=file:$XPCS_HADOOP_DIR/log4j.properties"

#[ -f clean.sh ] && ./clean.sh

version='0.5.0'

hadoop_job="$XPCS_HADOOP_DIR/xpcs-hadoop-$version-all.jar"

# The pipeline process doesn't handle a large information (if thrown by hadoop process)
# on the standard out or standard error. For now, dump all that to /dev/nul
#hadoop jar $hadoop_job gov.anl.aps.xpcs.main.Application $params 2>/dev/null

# Uncomment the line below and comment the one above for better debugging.
hadoop jar $hadoop_job $mainClass $params

ecode=$?

exit $ecode
