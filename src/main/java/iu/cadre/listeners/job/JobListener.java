package iu.cadre.listeners.job;

import com.google.gson.JsonParser;
import iu.cadre.listeners.job.util.ConfigReader;
import iu.cadre.listeners.job.util.ListenerUtils;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

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
        while (true) {
            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
            JobStatus status = new JobStatus();
            // print out the messages
            for (Message m : messages) {
                String messageBody = m.body();
                UserQuery query = new UserQuery(jsonParser.parse(messageBody).getAsJsonObject());
                Class.forName("org.postgresql.Driver");
                GraphTraversalSource janusTraversal = null;

                try {
                    status.Update(query.JobId(), "RUNNING");

                    String efsRootDir = ConfigReader.getEFSRootListenerDir();
                    String efsSubPath = ConfigReader.getEFSSubPathListenerDir();
                    String efsPath = efsRootDir + File.separator + efsSubPath;
                    String userQueryResultDir = efsPath + '/' + query.UserName() + "/query-results";
                    File directory = new File(userQueryResultDir);
                    if (!directory.exists()) {
                        directory.mkdirs();
                    }
                    String fileName = getFileName(query.JobId(), query.JobName());
                    String csvPath = userQueryResultDir + File.separator + fileName + ".csv";
                    String graphMLFile = userQueryResultDir + File.separator + fileName + ".xml";

                    if (query.DataSet().equals("mag")) {
                        LOG.info("Connecting to Janus server");
                        Cluster cluster = Cluster.build()
                                .addContactPoint("localhost")
                                .port(8182)
                                .maxContentLength(10000000)
                                .serializer(new GryoMessageSerializerV3d0(GryoMapper.build()
                                        .addRegistry(JanusGraphIoRegistry.instance())
                                        .addRegistry(TinkerIoRegistryV3d0.instance())))
                                .create();
                        janusTraversal = traversal().withRemote(DriverRemoteConnection.using(cluster));
                        // janusTraversal = (GraphTraversalSource) EmptyGraph.instance().configuration()..traversal().withRemote("conf/remote-graph.properties");

                        TinkerGraph tg = UserQuery2Gremlin.getSubGraphForQuery(janusTraversal, query, outputFiltersSingle);
                        GraphTraversalSource sg = tg.traversal();
                        LOG.info("Graph result received, writing GraphML to " + graphMLFile);
                        sg.io(graphMLFile).write().iterate();
                        //  to convert to csv
                        OutputStream stream = new FileOutputStream(csvPath);
                        GremlinGraphWriter.graph_to_csv(tg, stream);
                        janusTraversal.close();
                        janusTraversal = null;
                        LOG.info("Janus query complete");
                    }
                    String csvChecksum = ListenerUtils.getChecksum(csvPath);
                    String graphMLChecksum = ListenerUtils.getChecksum(graphMLFile);
                    status.Update(query.JobId(), "COMPLETED");

                    status.AddQueryResult(query.JobId(), query.UserId(), csvPath, csvChecksum);
                    status.AddQueryResult(query.JobId(), query.UserId(), graphMLFile, graphMLChecksum);
                } catch (CompletionException e) {
                    // Error with Janus. Log it, mark the job failed and keep going
                    LOG.error("Error reading JanusGraph: " + e.getMessage());
                    status.Update(query.JobId(), "FAILED");
                } catch (SQLException e) {
                    // Error with the metadatabase. Very bad since we can't report that
                    // the user's job failed. Exit the loop
                    throw e;
                } catch(Exception e) {
                    // Unknown error. We know it's not with the metadatabase as that
                    // would have been caught above, so mark the job failed and exit
                    // the loop
                    status.Update(query.JobId(), "FAILED");
                    throw e;
                }
                finally {
                    if (janusTraversal != null) janusTraversal.close();
                    LOG.info("Deleting the sqs message");
                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(m.receiptHandle())
                            .build();
                    sqsClient.deleteMessage(deleteMessageRequest);
                }

           }
            status.Close();
        }
    }

}

