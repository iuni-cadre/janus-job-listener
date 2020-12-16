JAR_NAME=janus-job-listener-0.0.1-SNAPSHOT-jar-with-dependencies.jar
DEST_PATH=/home/$USER/lib
PROPERTIES_PATH=/home/$USER/lib/cadre_config.properties

java -cp $DEST_PATH/$JAR_NAME iu.cadre.listeners.job.JobListener $PROPERTIES_PATH -Xms512M -Xmx1G
