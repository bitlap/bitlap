FROM openjdk:11-jdk
 
LABEL version="1.0" author="dreamylost" email="dreamylost@outlook.com" 
 
ARG bitlap_server=bitlap-0.4.0-SNAPSHOT 
ENV folder=/tmp/bitlap

RUN mkdir $folder
RUN mkdir -p $folder/$bitlap_server/static

WORKDIR $folder

COPY $bitlap_server.tar.gz  $folder/
COPY initFileForTest.sql  $folder/
COPY static/ $folder/$bitlap_server/static
COPY entrypoint.sh $folder/$bitlap_server

RUN tar -zxvf $folder/$bitlap_server.tar.gz
RUN chmod +777 $folder/$bitlap_server/entrypoint.sh

EXPOSE 18081
EXPOSE 12222
EXPOSE 23333


WORKDIR $folder/$bitlap_server

ENTRYPOINT ["./entrypoint.sh"]