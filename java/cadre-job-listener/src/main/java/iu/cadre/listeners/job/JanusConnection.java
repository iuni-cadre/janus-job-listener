package iu.cadre.listeners.job;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.Iterator;

public class JanusConnection {
    public static void main(String[] args) {
        try {
//            JanusGraphFactory.
//            GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("conf/remote-objects.yaml", "mag_traversal"));
//            GraphTraversal<Vertex, Map<Object, Object>> valueMap = g.V().has("Paper", "paperTitle", "full case study report upplandsbondens sweden")
//                    .valueMap("year", "paperTitle", "referenceCount", "rank", "citationCount", "createdDate", "paperId",
//                            "originalTitle", "date", "estimatedCitation", "languageCodes", "urls");
//            valueMap.forEachRemaining(
//                    System.out::println
//            );
//            g.close();

            if (null == args || args.length != 1) {
                System.err.println(
                        "Usage: JanusGraphConnSample <janusgraph-config-file>");
                System.exit(1);
            }

            final JanusGraph graph = JanusGraphFactory.open(args[0]);

            StandardJanusGraph standardGraph = (StandardJanusGraph) graph;

            // get graph management
            JanusGraphManagement mgmt = standardGraph.openManagement();

            // you code using 'mgmt' to perform any management related operations
            // bla bla bla

            // using graph to do traversal
            JanusGraphTransaction graphTransaction = graph.newTransaction();

            GraphTraversalSource traversal = graphTransaction.traversal();

            String vertexLabel = "Paper";
            String fieldName = "paperTitle";
            String fieldValue = "big data technologies a survey";

            Iterator<Vertex> nodes = traversal.V().has(vertexLabel, fieldName,
                    fieldValue);

            int count = 0;
            while (nodes.hasNext()) {
                Vertex v = nodes.next();
                count++;
                // use apis on Vertex to explore the node, below is the API link
                // http://tinkerpop.apache.org/javadocs/3.2.3/full/org/apache/tinkerpop/gremlin/structure/Vertex.html#properties-java.lang.String...-
            }
            System.out.println(count);

            // if you modify anything, always commit, for read only query, it is not needed
            // graphTransaction.commit();

            // close transaction when finishing using it
            graphTransaction.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Reuse 'g' across the application
        // and close it on shut-down to close open connections with g.close()
    }
}
