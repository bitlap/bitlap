#!/usr/bin/env bash


./bin/bitlap server restart

sleep 5


tail -500f logs/bitlap.log