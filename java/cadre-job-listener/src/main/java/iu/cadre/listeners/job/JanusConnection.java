package iu.cadre.listeners.job;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;

public class JanusConnection {
    public static void main(String[] args) {
        try {
            GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("conf/remote-objects.yaml  ", "mag_traversal"));
            GraphTraversal<Vertex, Map<Object, Object>> valueMap = g.V().has("Paper", "paperTitle", "full case study report upplandsbondens sweden")
                    .valueMap("year", "paperTitle", "referenceCount", "rank", "citationCount", "createdDate", "paperId",
                            "originalTitle", "date", "estimatedCitation", "languageCodes", "urls");
            valueMap.forEachRemaining(
                    System.out::println
            );
            g.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Reuse 'g' across the application
        // and close it on shut-down to close open connections with g.close()
    }
}
