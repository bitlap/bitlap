#!/usr/bin/env bash

tag=$1

cd $(dirname $0)/../

mkdir -p dist/
find dist/ -name 'bitlap*.tar.gz' | xargs rm -f

# make tar
TAR_FILE="bitlap-server/target/bitlap*.tar.gz"
cmd="./mvnw clean package -DskipTests -Passembly -Pwebapp -am -pl bitlap-server"
echo "========================================================================================================================================"
echo "==================  🔥 package start: ${cmd}  ============================"
echo "========================================================================================================================================"
eval ${cmd}

# move to dist directory
if [[ $? -eq 0 ]]; then
  mv ${TAR_FILE} docker/
  # 拷贝静态文件
  mv bitlap-server/target/classes/static docker/static
  echo "=============================================================================="
  echo "===============  🎉 package end in docker directory !!!  ======================="
  echo "=============================================================================="
fi
pwd
# 拷贝初始化SQL
cp ./conf/initFileForTest.sql ./docker/initFileForTest.sql

# 构建镜像
docker buildx build --build-arg bitlap_server=bitlap-$tag . -t bitlap:$tag --cache-to type=inline \
--cache-from type=registry,ref=bitlap/bitlap:${{ vars.VERSION }} -f ./Dockerfile

echo "===============  🎉 build image successfully !!!  ======================="

# 运行server，运行交互式sql（阻止容器退出）
#docker run --name bitlap:$tag -dit -p 18081:18081 -p 23333:23333 -p 12222:12222  bitlap:$tag

#echo "===============  🎉 bitlap_server running successfully !!!  ======================="
