#!/bin/sh
POWERCOS=$1
LOCATIONS=$2
TPS=$3
METERS=$4
DURATION=$5
QUERYSECONDS=20

PCO=0
LOC=0

cd ../jars 

while
	[ $PCO -le $POWERCOS ]
do
	java -jar voltdb-edge-powerco.jar `cat $HOME/.vdbhostnames`  ${TPS} ${DURATION} ${METERS} ${QUERYSECONDS}  $PCO > powerrco_${PCO}.lst  &
	PCO=`expr $PCO + 1`
done

while
	[ $LOC -le $LOCATIONS ]
do
	java -jar voltdb-edge-devices.jar `cat $HOME/.vdbhostnames`  ${DURATION} ${LOC} > location_${LOC}.lst  & 
	LOC=`expr $LOC + 1`
done

wait

