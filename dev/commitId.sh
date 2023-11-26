#!/bin/bash

cat bitlap-server/target/classes/git.properties | grep 'git.commit.id.abbrev=' | awk '{print substr($1,22)}'