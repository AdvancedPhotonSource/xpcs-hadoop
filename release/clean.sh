#!/bin/bash

if [ -d output ];
then
   echo 'Deleting and creating output folder'
   rm -rf output
else
   echo 'Creating output folder'
fi
mkdir output

#########

if hadoop fs -ls output/results > /dev/null
then
   echo "There is a folder, deleting it"
   hadoop fs -rmr output/results
else
   echo "No output folder"
fi

