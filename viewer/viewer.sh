#!/bin/bash

export HADOOP_CLASSPATH="/usr/lib/hadoop/hadoop-core.jar:/usr/lib/hbase/lib/commons-lang-2.5.jar:/usr/lib/hbase/lib/commons-logging-1.1.1.jar:/usr/lib/hbase/lib/zookeeper.jar:/usr/lib/hbase/hbase-0.90.4-cdh3u2.jar:/etc/hadoop/conf/"


function die {
	echo $1
	exit 1
}

if [ $# -eq 2 ]; then
	SERVER="$1"
	HBASE_TABLE="$2"

	echo "starting viewer"
	hadoop jar ../bin/dda.jar dda.viewer.DDAViewer $SERVER $HBASE_TABLE 1
else 
	die "you need to run this script as ./viewer.sh <server> <hbase-table>"
fi
