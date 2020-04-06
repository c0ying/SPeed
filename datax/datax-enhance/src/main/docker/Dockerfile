FROM centos:7

#USER 1000:1000
#JDK
ADD dependcies/jdk.tar.gz /opt/
COPY dependcies/SPeed/ /opt/SPeed/
COPY dependcies/entrypoint.sh /opt/SPeed/bin/

ENV JAVA_HOME=/opt/jdk
ENV PATH=$JAVA_HOME/bin:$PATH
ENV CLASS_PATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
ENV DATAX_HOME=/opt/SPeed

WORKDIR /opt
RUN ["chown","-R","1000:1000","SPeed"]

EXPOSE 8081

USER 1000:1000

WORKDIR $DATAX_HOME/bin

ENTRYPOINT ["./entrypoint.sh"]
