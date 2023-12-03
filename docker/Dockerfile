FROM openjdk:11-jdk
 
LABEL version="1.0" author="dreamylost" email="dreamylost@outlook.com" 
 
ARG bitlap_server=bitlap-0.4.0 
ENV folder=/tmp/bitlap

RUN mkdir $folder

WORKDIR $folder

COPY $bitlap_server.tar.gz  $folder/
COPY entrypoint.sh $folder/$bitlap_server

RUN tar -zxvf $folder/$bitlap_server.tar.gz
RUN chmod +777 $folder/$bitlap_server/entrypoint.sh

EXPOSE 22333
EXPOSE 23333
EXPOSE 24333


WORKDIR $folder/$bitlap_server

ENTRYPOINT ["./entrypoint.sh"]