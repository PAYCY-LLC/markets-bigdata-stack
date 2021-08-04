FROM openjdk:11-slim

# trino version will be passed in at build time
ARG TRINO_VERSION
ENV TRINO_VERSION=${TRINO_VERSION:-360}
ENV TRINO_HOME=/opt/trino


# Set the URL to download
ARG TRINO_BIN=https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz



# Update the base image OS and install wget and python
RUN apt-get update
RUN apt-get install -y wget python less

# Download Trino and unpack it to /opt/trino
RUN wget --quiet ${TRINO_BIN}
RUN mkdir -p /opt
RUN tar -xf trino-server-${TRINO_VERSION}.tar.gz -C /opt
RUN rm trino-server-${TRINO_VERSION}.tar.gz
RUN ln -s /opt/trino-server-${TRINO_VERSION} ${TRINO_HOME}

# Copy configuration files on the host into the image
COPY etc ${TRINO_HOME}/etc

# SerDE dependency
RUN wget --quiet http://www.congiu.net/hive-json-serde/1.3.8/hdp23/json-serde-1.3.8-jar-with-dependencies.jar -o ${TRINO_HOME}/plugin/hive-hadoop2/json-serde-1.3.8-jar-with-dependencies.jar

# Download the trino CLI and put it in the image
RUN wget --quiet https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar
RUN mv trino-cli-${TRINO_VERSION}-executable.jar /usr/local/bin/trino
RUN chmod +x /usr/local/bin/trino

# Specify the entrypoint to start
ENTRYPOINT ${TRINO_HOME}/bin/launcher run