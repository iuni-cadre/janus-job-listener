package iu.cadre.listeners.job;

import com.google.gson.JsonParser;
import iu.cadre.listeners.job.util.ConfigReader;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static iu.cadre.listeners.job.UserQuery2Gremlin.record_limit;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;


public class JanusConnection {
    protected static final Log LOG = LogFactory.getLog(JanusConnection.class);

    public static void main(String[] args) {
        if (null == args || args.length != 2) {
            System.err.println(
                    "Usage: JanusConnection config.properties file.json");
            System.exit(1);
        }

        try (Reader reader = Files.newBufferedReader(Paths.get(args[1]),
                StandardCharsets.UTF_8)) {

            ConfigReader.loadProperties(args[0]);
            JsonParser parser = new JsonParser();
            UserQuery query = new UserQuery(parser.parse(reader).getAsJsonObject());
            LOG.info("Read query: " + query.toString());

            String fileNameWithOutExtension = FilenameUtils.removeExtension(args[1]);
            Request(query, fileNameWithOutExtension + ".csv", fileNameWithOutExtension + "_edges.csv");
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static GraphTraversalSource getJanusMAGTraversal() throws Exception{
        try {
            String janusConfig = ConfigReader.getJanusMAGPropertiesFile();
            final JanusGraph graph = JanusGraphFactory.open(janusConfig);
            StandardJanusGraph standardGraph = (StandardJanusGraph) graph;
            // get graph management
            JanusGraphManagement mgmt = standardGraph.openManagement();
            // you code using 'mgmt' to perform any management related operations
            // using graph to do traversal
            JanusGraphTransaction graphTransaction = graph.newTransaction();
            return graphTransaction.traversal();
        }catch (Exception e){
            LOG.error( "Unable to create graph traversal object. Error : " +e.getMessage());
            throw new Exception("Unable to create graph traversal object.", e);
        }
    }

    public static GraphTraversalSource getJanusWOSTraversal() throws Exception{
        try {
            String janusConfig = ConfigReader.getJanusWOSPropertiesFile();
            final JanusGraph graph = JanusGraphFactory.open(janusConfig);
            StandardJanusGraph standardGraph = (StandardJanusGraph) graph;
            // get graph management
            JanusGraphManagement mgmt = standardGraph.openManagement();
            // you code using 'mgmt' to perform any management related operations
            // using graph to do traversal
            JanusGraphTransaction graphTransaction = graph.newTransaction();
            return graphTransaction.traversal();
        }catch (Exception e){
            LOG.error( "Unable to create graph traversal object. Error : " +e.getMessage());
            throw new Exception("Unable to create graph traversal object.", e);
        }
    }

    public static void Request(UserQuery query, String edgesCSVPath, String verticesCSVPath) throws Exception {
        LOG.info("Connecting to Janus server");
        String server = ConfigReader.getJanusHost();
        int port = 8182;
        GraphTraversalSource janusTraversal = null;
        if (query.DataSet().equals("mag")){
            janusTraversal = getJanusMAGTraversal();

        }else {
            janusTraversal = getJanusWOSTraversal();
        }
        Cluster cluster = Cluster.build()
                .addContactPoint(server)
                .port(port)
                .maxContentLength(100000000)
                .serializer(new GryoMessageSerializerV3d0(GryoMapper.build()
                        .addRegistry(JanusGraphIoRegistry.instance())
                        .addRegistry(TinkerIoRegistryV3d0.instance())))
                .create();



        record_limit = ConfigReader.getJanusRecordLimit();
        OutputStream verticesStream = new FileOutputStream(verticesCSVPath);

        int batchSize = 100; // we need some test to figure out the best batchSize to use, I just make up a number here
        List<Map> t1Elements = new ArrayList<>();
        List<Map> t2Elements = new ArrayList<>();
        List<Map> t3Elements = new ArrayList<>();
        GraphTraversal t1 = null;
        GraphTraversal t2 = null;
        GraphTraversal t = null;

        if (query.DataSet().equals("mag")){
            t = UserQuery2Gremlin.getMAGProjectionForQuery(janusTraversal, query);
        }else {
            t = UserQuery2Gremlin.getWOSProjectionForQuery(janusTraversal, query);
        }

        t = t.limit(record_limit).as("a");
        if (query.RequiresGraph()) {
            OutputStream edgesStream = new FileOutputStream(edgesCSVPath);
            t1 = UserQuery2Gremlin.getPaperProjection(t.asAdmin().clone().outE("References").bothV().dedup(), query);
            LOG.info("Query1 " + t1);
            while (t1.hasNext()) {
                t1Elements.addAll(t1.next(batchSize));
            }
            GremlinGraphWriter.projection_to_csv(t1Elements, verticesStream);
            t2 = UserQuery2Gremlin.getPaperProjectionForNetwork(t.asAdmin().clone().outE("References"), query);
            LOG.info("Query2 " + t2);

            while (t2.hasNext()) {
                t2Elements.addAll(t2.next(batchSize));
            }
            GremlinGraphWriter.projection_to_csv(t2Elements, edgesStream);
        } else {
            t = UserQuery2Gremlin.getPaperProjection(t, query);
            while (t.hasNext()) {
                t3Elements.addAll(t.next(batchSize));
            }
            GremlinGraphWriter.projection_to_csv(t3Elements, verticesStream);
        }

        janusTraversal.close();
        LOG.info("Janus query complete");
    }


}
