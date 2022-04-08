#!/bin/sh

cd /home/ubuntu
. ./.profile

cd voltdb-edge/scripts

sleep 120
sqlcmd --servers=`cat $HOME/.vdbhostnames` < ../ddl/voltdb-edge-createDB.sql
java -jar $HOME/bin/addtodeploymentdotxml.jar `cat $HOME/.vdbhostnames` deployment topics.xml
