package iu.cadre.listeners.job;

import com.google.gson.JsonParser;
import iu.cadre.listeners.job.util.ConfigReader;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

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
            String fileNameWithOutExtension = FilenameUtils.removeExtension(args[1]);
            Request(query, fileNameWithOutExtension + ".ml", fileNameWithOutExtension + ".csv");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void Request(UserQuery query, String graphMLFile, String csvPath) throws Exception {
        LOG.info("Connecting to Janus server");
        Cluster cluster = Cluster.build()
                .addContactPoint(ConfigReader.getJanusHost())
                .port(8182)
                .maxContentLength(10000000)
                .serializer(new GryoMessageSerializerV3d0(GryoMapper.build()
                        .addRegistry(JanusGraphIoRegistry.instance())
                        .addRegistry(TinkerIoRegistryV3d0.instance())))
                .create();
        GraphTraversalSource janusTraversal = traversal().withRemote(DriverRemoteConnection.using(cluster));

        TinkerGraph tg = UserQuery2Gremlin.getSubGraphForQuery(janusTraversal, query);
        GraphTraversalSource sg = tg.traversal();
        LOG.info("Graph result received, writing GraphML to " + graphMLFile);
        sg.io(graphMLFile).write().iterate();
        //  to convert to csv
        OutputStream stream = new FileOutputStream(csvPath);
        GremlinGraphWriter.graph_to_csv(tg, stream);
        janusTraversal.close();
        LOG.info("Janus query complete");
    }


}
