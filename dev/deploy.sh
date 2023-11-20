#!/bin/bash

version=$1
#sh dev/make-tarball.sh

scp bitlap-server/target/bitlap-$1.tar.gz root@ip:/root/bitlap-$1.tar.gz

cd bitlap-server/target/classes

tar -zcvf static.tar.gz ./static

scp static.tar.gz root@ip:/root/bitlap-$1/static.tar.gz
ssh root@ip "cd /root/bitlap-$1; tar -zxvf static.tar.gz;"

ssh root@ip "cd /root; tar -zxvf bitlap-$1.tar.gz; cp bitlap-$1/conf/initFileForTest.sql bitlap-$1/initFileForTest.sql; cd bitlap-$1; nohup bin/bitlap server restart > /dev/null 2>&1"
