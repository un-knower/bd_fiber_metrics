#!/bin/bash
cd  /data/workspace/sparkJar/bd_fiber_metrics/apps
PID=$(ps -ef | grep *.jar | grep -v grep | awk 'NR==1{ print $2 }')

if [ -z "$PID" ]
then 
	echo "application is already stopped"
	echo $PID
else    
	echo "kill $PID"
	kill $PID
fi	
