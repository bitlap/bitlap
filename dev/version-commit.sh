#!/bin/bash

# only available for snapshots

version=$1
commit=$2

num=`echo $version | awk -F '-' {'print $1'}`
newVersion=$num-$commit-SNAPSHOT
echo $newVersion