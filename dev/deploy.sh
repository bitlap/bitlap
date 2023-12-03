#!/bin/bash

version=$1

cd $(dirname $0)/../

./mvnw compile

# only available for snapshots
commitId=`sh dev/commitId.sh`
newVersion=`sh dev/version-commit.sh $version $commitId` 

sh dev/make-tarball.sh $newVersion

if [[ $? -eq 0 ]]; then
  scp dist/bitlap-$newVersion.tar.gz root@ip:/root/bitlap-$newVersion.tar.gz
  echo "=============================================================================="
  echo "===============  🎉 Upload end  !!!  ========================================="
  echo "=============================================================================="
  
  ssh root@ip "cd /root; tar -zxvf bitlap-$newVersion.tar.gz; cd bitlap-$newVersion; nohup bin/bitlap server restart > /dev/null 2>&1"
  
  echo "=============================================================================="
  echo "===============  🎉 Deploy end  !!!  ========================================="
  echo "=============================================================================="
fi