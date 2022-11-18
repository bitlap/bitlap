#!/bin/bash

version=$1
sh dev/make-tarball.sh
cd dist
scp bitlap-$1.tar.gz root@your_ip:/root/bitlap-$1.tar.gz
ssh root@your_ip "cd /root; tar -zxvf bitlap-$1.tar.gz; nohup ./bitlap-$1.tar/bin/bitlap server restart > /dev/null 2>&1"
