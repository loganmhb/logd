FROM openjdk:latest 
ADD target/logd-0.1.0-SNAPSHOT-standalone.jar /srv/logd/
CMD java -jar /srv/logd/logd-0.1.0-SNAPSHOT-standalone.jar
