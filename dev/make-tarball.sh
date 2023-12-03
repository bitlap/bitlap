#!/usr/bin/env bash

version=$1

cd $(dirname $0)/../

# init dist directory
mkdir -p dist/
find dist/ -name 'bitlap*.tar.gz' | xargs rm -f

# make tar
TAR_FILE="bitlap-server/target/bitlap*.tar.gz"
cmd="./mvnw clean package -DskipTests -Passembly -Pwebapp -Drevision=${version} -am -pl bitlap-server"
echo "========================================================================================================================================"
echo "==================  🔥 package start: ${cmd}  ============================"
echo "========================================================================================================================================"
eval ${cmd}

# move to dist directory
if [[ $? -eq 0 ]]; then
  mv ${TAR_FILE} dist/
  echo "=============================================================================="
  echo "===============  🎉 package end in dist directory !!!  ======================="
  echo "=============================================================================="
fi