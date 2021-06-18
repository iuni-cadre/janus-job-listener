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


public class JobListener {
    private static final String QUEUE_NAME = "cadre-janus-queue.fifo";
    private static final JsonParser jsonParser = new JsonParser();
    private static final Logger LOG = LoggerFactory.getLogger(JobListener.class);

    public static void main(String[] args) {
        try {
            ConfigReader.loadProperties(args[0]);
            poll_queue();
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

    public static void poll_queue() throws Exception {
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
            JobStatus status = new JobStatus(ConfigReader.getMetaDBInMemory());
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

                try {
                    status.Update(query.JobId(), "RUNNING", "");

                    Path userQueryResultDir = FileSystems.getDefault().getPath(ConfigReader.getEFSRootListenerDir(),
                            ConfigReader.getEFSSubPathListenerDir(),
                            query.UserName(), "query-results");
                    Files.createDirectories(userQueryResultDir);

                    String fileName = getFileName(query.JobId(), query.JobName());
                    String verticesCSVPath = userQueryResultDir + File.separator + fileName + ".csv";
                    String edgesCSVPath = userQueryResultDir + File.separator + fileName + "_edges.csv";

                    JanusConnection.Request(query, edgesCSVPath, verticesCSVPath);
                    String dataType = "MAG";
                    if (query.DataSet().equals("wos")){
                        dataType = "WOS";
                    }else if (query.DataSet().equals("uspto")){
                        dataType = "USPTO";
                    }

                    if (new File(verticesCSVPath).exists()) {
                        String csvChecksum = ListenerUtils.getChecksum(verticesCSVPath);
                        status.AddQueryResult(query.JobId(), query.UserId(), verticesCSVPath, csvChecksum, dataType);
                    }

                    if (new File(edgesCSVPath).exists()) {
                        String graphMLChecksum = ListenerUtils.getChecksum(edgesCSVPath);
                        status.AddQueryResult(query.JobId(), query.UserId(), edgesCSVPath, graphMLChecksum, dataType);
                    }

                    status.Update(query.JobId(), "COMPLETED", "");

                } catch (CompletionException e) {
                    // Error with Janus. Log it, mark the job failed and keep going
                    LOG.error("Error reading JanusGraph: " + e.getMessage());
                    status.Update(query.JobId(), "FAILED", e.getMessage());
                } catch (SQLException e) {
                    // Error with the metadatabase. Very bad since we can't report that
                    // the user's job failed. Exit the loop
                    throw e;
                } catch(Exception e) {
                    String msg = null;
                    // Unknown error. We know it's not with the metadatabase as that
                    // would have been caught above, so mark the job failed and exit
                    // the loop

                    if (e.getMessage().contains("method [POST]") &&
                        e.getMessage().contains("No search context found")) {
                       msg = "The system is experiencing a high volume or the query may have exceeded capacity limitations.  Please form a more specific query or try again later.";
                    } else {
                       msg = e.getMessage();
                    }
                       
                    status.Update(query.JobId(), "FAILED", msg);
                    throw e;
                }
           }
            status.Close();
        }
    }

}

