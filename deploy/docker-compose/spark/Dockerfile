FROM apache/spark:3.5.0

ENV PATH="${PATH}:/opt/spark/bin"

USER root
RUN mkdir -p /home/spark; chown spark:spark /home/spark
COPY rss-client-spark3-shaded-*.jar /opt/spark/jars
USER spark
