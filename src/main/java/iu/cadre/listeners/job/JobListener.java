package iu.cadre.listeners.job;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import iu.cadre.listeners.job.util.ConfigReader;
import iu.cadre.listeners.job.util.ListenerUtils;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JobListener {
    private static final String QUEUE_NAME = "cadre-janus-queue.fifo";
    private static final JsonParser jsonParser = new JsonParser();
    private static final Logger LOG = LoggerFactory.getLogger(JobListener.class);

    public static void main(String[] args) {
        try {
            ConfigReader.loadProperties(args[0]);
            poll_queue();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

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
        SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_2).build();
        List<String> outputFiltersSingle = new ArrayList<String>();
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(QUEUE_NAME)
                .build();

        String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();

        LOG.info("SQS connection established and listening");
        JobStatus status = new JobStatus();
        while (true) {
            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
            // print out the messages
            for (Message m : messages) {
                String messageBody = m.body();
                JsonElement messageBodyJElement = jsonParser.parse(messageBody);
                JsonObject messageBodyJObj = messageBodyJElement.getAsJsonObject();
                String dataset = messageBodyJObj.get("dataset").getAsString();
                String jobId = messageBodyJObj.get("job_id").getAsString();
                String jobName = messageBodyJObj.get("job_name").getAsString();
                String userName = messageBodyJObj.get("username").getAsString();
                String userId = messageBodyJObj.get("user_id").getAsString();
                JsonObject graphJson = messageBodyJObj.get("graph").getAsJsonObject();
                JsonArray outputFields = messageBodyJObj.get("csv_output").getAsJsonArray();
                Class.forName("org.postgresql.Driver");
                GraphTraversalSource janusTraversal = null;

                try {
                    status.Update(jobId, "RUNNING");

                    String efsRootDir = ConfigReader.getEFSRootListenerDir();
                    String efsSubPath = ConfigReader.getEFSSubPathListenerDir();
                    String efsPath = efsRootDir + File.separator + efsSubPath;
                    String userQueryResultDir = efsPath + '/' + userName + "/query-results";
                    File directory = new File(userQueryResultDir);
                    if (!directory.exists()) {
                        directory.mkdirs();
                    }
                    String fileName = getFileName(jobId, jobName);
                    String csvPath = userQueryResultDir + File.separator + fileName + ".csv";
                    String graphMLFile = userQueryResultDir + File.separator + fileName + ".xml";
                    LOG.info(graphMLFile);

                    if (dataset.equals("mag")) {
                        janusTraversal = (GraphTraversalSource) EmptyGraph.instance().traversal().withRemote("conf/remote-graph.properties");

                        TinkerGraph tg = JSON2Gremlin.getSubGraphForQuery(janusTraversal, graphJson, outputFiltersSingle);
                        GraphTraversalSource sg = tg.traversal();
                        sg.io(graphMLFile).write().iterate();
                        //  to convert to csv
                        OutputStream stream = new FileOutputStream(csvPath);
                        GremlinGraphWriter.graph_to_csv(tg, stream);
                        janusTraversal.close();
                    }
                    String csvChecksum = ListenerUtils.getChecksum(csvPath);
                    String graphMLChecksum = ListenerUtils.getChecksum(graphMLFile);
                    LOG.info("Deleting the sqs message");
                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(m.receiptHandle())
                            .build();
                    sqsClient.deleteMessage(deleteMessageRequest);
                    status.Update(jobId, "COMPLETED");

                    status.AddQueryResult(jobId, userId, csvPath, csvChecksum);
                    status.AddQueryResult(jobId, userId, graphMLFile, graphMLChecksum);
                } catch (ResponseException e) {
                    LOG.error("Error reading JanusGraph: " + e.getMessage());
                    status.Update(jobId, "FAILED");
                } catch (SQLException e) {
                    status.Update(jobId, "FAILED");
                    LOG.error("Error while updating meta db. Error is : " + e.getMessage());
                    throw new Exception("Error while updating meta db", e);
                }finally {
                    if (janusTraversal != null) janusTraversal.close();
                    status.Close();
                }
            }
        }
    }

}

