#!/bin/bash

# only available for snapshots
cd $(dirname $0)/../

cat bitlap-server/target/classes/git.properties | grep 'git.commit.id.abbrev=' | awk '{print substr($1,22)}'