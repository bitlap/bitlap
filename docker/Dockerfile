FROM openjdk:11-jdk
 
MAINTAINER dreamylost dreamylost@outlook.com
 
ENV bitlap=bitlap-0.3.0-SNAPSHOT 
ENV file=$bitlap.tar.gz
ENV folder=/tmp/bitlap

RUN mkdir $folder
WORKDIR $folder

COPY $file  $folder/
COPY initFileForTest.sql  $folder/
RUN tar -zxvf $folder/$file

COPY entrypoint.sh $folder/$bitlap

RUN mkdir -p $folder/$bitlap/static

COPY static/ $folder/$bitlap/static

EXPOSE 18081
EXPOSE 12222
EXPOSE 23333

RUN chmod +777 $folder/$bitlap/entrypoint.sh

WORKDIR $folder/$bitlap

ENTRYPOINT ["./entrypoint.sh"]