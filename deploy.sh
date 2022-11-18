#!/bin/bash

version=$1
sh dev/make-tarball.sh
cd dist
scp bitlap-$1.tar.gz root@your_ip:/root/bitlap-$1.tar.gz
ssh root@your_ip "cd /root; tar -zxvf bitlap-$1.tar.gz; cp bitlap-$1/conf/initFileForTest.sql bitlap-$1/initFileForTest.sql; cd bitlap-$1; nohup bin/bitlap server restart > /dev/null 2>&1"
