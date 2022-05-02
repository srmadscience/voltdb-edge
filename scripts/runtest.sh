#!/bin/sh
POWERCOS=$1
LOCATIONS=$2
TPS=$3
METERS=$4
DURATION=$5
QUERYSECONDS=20

PCO=0
LOC=0
DT=`date '+%Y%m%d_%H%M'`

cd ../jars

kill `ps -deaf | grep java | grep voltdb-edge | awk '{ print $2 }'` 2> /dev/null
sleep 5

while
        [ $LOC -lt $LOCATIONS ]
do
        java -jar voltdb-edge-devices.jar `cat $HOME/.vdbhostnames`  ${DURATION} ${LOC} > location_${LOC}_${DT}_${POWERCOS}_${LOCATIONS}_${TPS}_${METERS}.lst  &
        LOC=`expr $LOC + 1`
done

sleep 5

while
        [ $PCO -lt $POWERCOS ]
do
        java -jar voltdb-edge-powerco.jar `cat $HOME/.vdbhostnames`  ${TPS} ${DURATION} ${METERS} ${QUERYSECONDS}  $PCO > powerco_${PCO}_${DT}_${POWERCOS}_${LOCATIONS}_${TPS}_${METERS}.lst  &
        PCO=`expr $PCO + 1`
done


wait
