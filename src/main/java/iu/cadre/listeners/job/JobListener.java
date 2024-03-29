package iu.cadre.listeners.job;

import com.google.gson.JsonParser;
import iu.cadre.listeners.job.util.ConfigReader;
import iu.cadre.listeners.job.util.ListenerUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.lang.Runtime;


public class JobListener {
    private static final String QUEUE_NAME = "cadre-janus-queue.fifo";
    private static final JsonParser jsonParser = new JsonParser();
    private static final Logger LOG = LoggerFactory.getLogger(JobListener.class);
    private static final long FIRST_CLUSTER_WAIT_TIME = 180000; // 180 sec
    private static final long CLUSTER_WAIT_TIME = 30000; // 30 sec
    private static final long POLL_QUEUE_SLEEP_TIME = 30000; // 30sec in ms
    private static final int MAX_REQUEST_ATTEMPTS = 6;
    private static int listenerID;

    public static void main(String[] args) {
        try {
            ConfigReader.loadProperties(args[0]);
            listenerID = Integer.parseInt(args[1]);
 
            ListenerStatus listenerStatus = new ListenerStatus(listenerID,
                                                   ConfigReader.getMetaDBInMemory());
            Runtime.getRuntime().addShutdownHook(new Thread(
               new JobListenerInterruptHandler(listenerStatus)));
            listenerStatus.update(ListenerStatus.CLUSTER_NONE, ListenerStatus.STATUS_IDLE);
            poll_queue(listenerStatus);
            listenerStatus.update(ListenerStatus.CLUSTER_NONE, ListenerStatus.STATUS_STOPPED);
            System.exit(0);
        } catch (Exception e) {
            LOG.error("FATAL ERROR, exiting", e);
            System.exit(-1);
        }
    }

    public static String getFileName(String jobID, String jobName){
        String fileName = "";
        if (jobID.equals(jobName) || jobName.isEmpty()){
           fileName = jobID;
        }else {
           fileName = jobName + '_' + jobID;
        }
        return fileName;
    }

    public static void poll_queue(ListenerStatus listenerStatus) throws Exception {
        Class.forName("org.postgresql.Driver");

        SqsClient sqsClient = SqsClient.builder().region(ConfigReader.getAwsRegion()).build();
        List<String> outputFiltersSingle = new ArrayList<String>();
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(QUEUE_NAME)
                .build();

        String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();

        LOG.info("SQS connection established and listening");
        while (true) {
            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

            if (messages.size() == 0) {
               Thread.sleep(POLL_QUEUE_SLEEP_TIME);
               continue;
            }

            JobStatus jobStatus = new JobStatus(ConfigReader.getMetaDBInMemory());
            // The DB could have dropped the connection
            listenerStatus.refreshConnection();

            LOG.info("receiveMessages received " + messages.size() + " messages");
            // print out the messages
            for (Message m : messages) {
                LOG.info(m.body());
                UserQuery query = new UserQuery(jsonParser.parse(m.body()).getAsJsonObject());
                LOG.info("Accepted " + query.JobId() + " for " + query.UserName());
                LOG.info(query.toString());
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(m.receiptHandle())
                        .build();
                sqsClient.deleteMessage(deleteMessageRequest);
                String dataType = "MAG";

                try {
                    jobStatus.Update(query.JobId(), "RUNNING", "");

                    Path userQueryResultDir = FileSystems.getDefault().getPath(ConfigReader.getEFSRootListenerDir(),
                            ConfigReader.getEFSSubPathListenerDir(),
                            query.UserName(), "query-results");
                    Files.createDirectories(userQueryResultDir);

                    String fileName = getFileName(query.JobId(), query.JobName());
                    String verticesCSVPath = userQueryResultDir + File.separator + fileName + ".csv";
                    String edgesCSVPath = userQueryResultDir + File.separator + fileName + "_edges.csv";

                    if (query.DataSet().equals("wos")){
                        dataType = "WOS";
                    }else if (query.DataSet().equals("uspto")){
                        dataType = "USPTO";
                    }

                    listenerStatus.update(dataType, ListenerStatus.STATUS_RUNNING);

                    // If the listener fails to connect with a Janus cluster, it might be
                    // becuase the cluster has not fully started up.  This loop attempts
                    // to process the request until the Janus connection can be made or
                    // the maximum limits on attempts is reached or an exeption occurs
                    // that prevents processing of the query.
                    for (int requestAttempt = 1; requestAttempt <= MAX_REQUEST_ATTEMPTS; requestAttempt++) {
                        try {
                           JanusConnection.Request(query, edgesCSVPath, verticesCSVPath);
                           break;
                        } catch (ClusterNotInitializedException e) {
                             long waitTime;

                             if (requestAttempt == MAX_REQUEST_ATTEMPTS) {
                                 throw e;
                             }

                             waitTime = (requestAttempt == 1) ? FIRST_CLUSTER_WAIT_TIME : CLUSTER_WAIT_TIME;
                             LOG.warn("Cluster not started.  Sleeping for " + waitTime/1000.0
                                 + " seconds: " + e.getMessage());
                             Thread.sleep(waitTime);
                        } catch (Exception e) {
                           throw e;
                        }
                    }
 
                    if (new File(verticesCSVPath).exists()) {
                        String csvChecksum = ListenerUtils.getChecksum(verticesCSVPath);
                        jobStatus.AddQueryResult(query.JobId(), query.UserId(), verticesCSVPath, csvChecksum, dataType);
                    }

                    if (new File(edgesCSVPath).exists()) {
                        String graphMLChecksum = ListenerUtils.getChecksum(edgesCSVPath);
                        jobStatus.AddQueryResult(query.JobId(), query.UserId(), edgesCSVPath, graphMLChecksum, dataType);
                    }

                    jobStatus.Update(query.JobId(), "COMPLETED", "");
                    listenerStatus.update(dataType, ListenerStatus.STATUS_IDLE);
                } catch (ClusterNotInitializedException e) {
                    LOG.warn("Cluster not fully started and initialized: " + e.getMessage());
                    jobStatus.Update(query.JobId(), "FAILED", e.getMessage());
                    listenerStatus.update(dataType, ListenerStatus.STATUS_IDLE);
                } catch (CompletionException e) {
                    // Error with Janus. Log it, mark the job failed and keep going
                    LOG.error("Error reading JanusGraph: " + e.getMessage());
                    jobStatus.Update(query.JobId(), "FAILED", e.getMessage());
                    listenerStatus.update(dataType, ListenerStatus.STATUS_IDLE);
                } catch (SQLException e) {
                    // Error with the metadatabase. Very bad since we can't report that
                    // the user's job failed. Exit the loop
                    throw e;
                } catch(Exception e) {
                    boolean rethrow = false;
                    String msg = null;
                    // Unknown error. We know it's not with the metadatabase as that
                    // would have been caught above, so mark the job failed and exit
                    // the loop

                    if (e.getMessage().contains("method [POST]") &&
                        e.getMessage().contains("No search context found")) {
                       msg = "The system is experiencing a high volume or the query may have exceeded capacity limitations.  Please form a more specific query or try again later.";
                    } else {
                       rethrow = true;
                       msg = e.getMessage();
                    }

                    jobStatus.Update(query.JobId(), "FAILED", msg);
                    listenerStatus.update(dataType, ListenerStatus.STATUS_IDLE);

                    // Rethrow serious exceptions
                    if (rethrow) {
                       throw e;
                    }
                }
            }
            jobStatus.Close();
        }

    }
}
