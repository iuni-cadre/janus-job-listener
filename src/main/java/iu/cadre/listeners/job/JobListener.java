package iu.cadre.listeners.job;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import iu.cadre.listeners.job.util.ConfigReader;
import iu.cadre.listeners.job.util.ListenerUtils;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JobListener {
    private static final String QUEUE_NAME = "cadre-janus-queue.fifo";
    private static final JsonParser jsonParser = new JsonParser();
    private static final Logger LOG = LoggerFactory.getLogger(JobListener.class);
    private static JanusGraph graph = null;
    private static JanusGraphTransaction graphTransaction = null;


    public static void main(String[] args) {
        try {
            ConfigReader.loadProperties(args[0]);
            poll_queue();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (graph != null){
                graphTransaction.close();
                graph.close();
            }
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

    public static GraphTraversalSource getJanusTraversal() throws Exception{
        try {
            String janusConfig = ConfigReader.getJanusPropertiesFile();
            graph = JanusGraphFactory.open(janusConfig);
            StandardJanusGraph standardGraph = (StandardJanusGraph) graph;
            // get graph management
            JanusGraphManagement mgmt = standardGraph.openManagement();
            // you code using 'mgmt' to perform any management related operations
            // using graph to do traversal
            graphTransaction = graph.newTransaction();
            return graphTransaction.traversal();
        }catch (Exception e){
            LOG.error( "Unable to create graph traversal object. Error : " +e.getMessage());
            throw new Exception("Unable to create graph traversal object.", e);
        }
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
        String jobUpdateStatement = "UPDATE user_job SET job_status = ?, modified_on = CURRENT_TIMESTAMP WHERE job_id = ?";
        String fileInsertStatement = "INSERT INTO query_result(job_id,efs_path, file_checksum, data_type, authenticity, created_by, created_on) " +
                "VALUES(?,?,?,?,?,?,current_timestamp)";
        LOG.info("SQS connection established and listening");
        String metaDBUrl = "jdbc:postgresql://" + ConfigReader.getMetaDBHost() + ":" + ConfigReader.getMetaDBPort() + "/" + ConfigReader.getMetaDBName();

        while (true) {
            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
            // print out the messages
            for (Message m : messages) {
                String messageBody = m.body();
                LOG.info(messageBody);
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
                Connection connection = DriverManager.getConnection(
                        metaDBUrl, ConfigReader.getMetaDBUsername(), ConfigReader.getMetaDBPWD());
                PreparedStatement jobStatusPreparedStatement = connection.prepareStatement(jobUpdateStatement);
                PreparedStatement fileInfoPreparedStatement = connection.prepareStatement(fileInsertStatement);
                try {
                    jobStatusPreparedStatement.setString(1, "RUNNING");
                    jobStatusPreparedStatement.setString(2, jobId);
                    jobStatusPreparedStatement.executeUpdate();

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
                        GraphTraversalSource janusTraversal = getJanusTraversal();
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
                    LOG.info("Updating job status in metadb to completed..");
                    sqsClient.deleteMessage(deleteMessageRequest);
                    jobStatusPreparedStatement.setString(1, "COMPLETED");
                    jobStatusPreparedStatement.setString(2, jobId);
                    jobStatusPreparedStatement.executeUpdate();

                    LOG.info("Updating csv file checksum in metadb to completed..");
                    fileInfoPreparedStatement.setString(1, jobId);
                    fileInfoPreparedStatement.setString(2, csvPath);
                    fileInfoPreparedStatement.setString(3, csvChecksum);
                    fileInfoPreparedStatement.setString(4, "MAG");
                    fileInfoPreparedStatement.setBoolean(5, true);
                    fileInfoPreparedStatement.setInt(6, Integer.parseInt(userId));
                    fileInfoPreparedStatement.executeUpdate();

                    LOG.info("Updating graphml checksum in metadb to completed..");
                    fileInfoPreparedStatement.setString(1, jobId);
                    fileInfoPreparedStatement.setString(2, graphMLFile);
                    fileInfoPreparedStatement.setString(3, graphMLChecksum);
                    fileInfoPreparedStatement.setString(4, "MAG");
                    fileInfoPreparedStatement.setBoolean(5, true);
                    fileInfoPreparedStatement.setInt(6, Integer.parseInt(userId));
                    fileInfoPreparedStatement.executeUpdate();

                } catch (SQLException e) {
                    LOG.info("Error occurred");
                    jobStatusPreparedStatement.setString(1, "FAILED");
                    jobStatusPreparedStatement.setString(2, jobId);
                    jobStatusPreparedStatement.executeUpdate();
                    LOG.error("Error while updating meta db. Error is : " + e.getMessage());
                    throw new Exception("Error while updating meta db", e);
                }finally {
                    jobStatusPreparedStatement.close();
                    fileInfoPreparedStatement.close();
                    connection.close();
                }
            }
        }
    }
}
