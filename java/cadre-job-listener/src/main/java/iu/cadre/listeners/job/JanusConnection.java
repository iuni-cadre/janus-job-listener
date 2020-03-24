package iu.cadre.listeners.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import static org.janusgraph.core.attribute.Text.textContainsFuzzy;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;

import java.util.Iterator;
import java.util.List;

public class JanusConnection {
    protected static final Log LOG = LogFactory.getLog(JanusConnection.class);
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


//            g.V().and(has('Paper','paperTitle', textContainsFuzzy('unicorns')), has('year', 1990)).valueMap()
            String vertexLabel = "Paper";
            String fieldName = "paperTitle";
            String fieldValue = "unicorns";

            GraphTraversal<Vertex, Vertex> v = traversal.V();

            int count = traversal.V().and(has("Paper", "paperTitle", textContainsFuzzy("unicorns")), has("year", 1990)).toList().size();
            System.out.println("************ COUNT ******** : " + count);

//            v.and(has('Paper','paperTitle', textContainsFuzzy('unicorns')), has('year', 1990)).inE().subgraph('unicorn').cap('unicorn').next()
            TinkerGraph tg = (TinkerGraph)traversal.V().and(has(vertexLabel, fieldName, textContainsFuzzy(fieldValue)), has(vertexLabel, "year", 1990)).inE("AuthorOf").subgraph("org_auth2").cap("org_auth2").next();
            GraphTraversalSource sg = tg.traversal();
            sg.io("/home/ubuntu/unicorn_chathuri_2.xml").write().iterate();
//            int count = 0;
//            while (nodes.hasNext()){
//                count++;
//                Vertex vertex = nodes.next();
//                System.out.println("Paper Title : " + vertex.property("paperTitle").value().toString());
//                System.out.println("Reference Count : " + vertex.property("referenceCount").value().toString());
//                System.out.println("Year : " + vertex.property("year").value().toString());
//                System.out.println("paperId : " + vertex.property("paperId").value().toString());
//            }
//            System.out.println("******** COUNT : **********" + count);

//            GraphTraversal<Vertex, Map<Object, Object>> vertexMapGraphTraversal = traversal.V().has("Paper", "paperTitle", "full case study report upplandsbondens sweden")
//                    .valueMap("year", "paperTitle", "referenceCount", "rank", "citationCount", "createdDate", "paperId",
//                            "originalTitle", "date", "estimatedCitation", "languageCodes", "urls");
//
//            int count = 0;
//            while (nodes.hasNext()) {
//                System.out.println("********** count ********** : " + count);
//                Vertex v = nodes.next();
//                System.out.println("************ TITLE ************ : " + v.property("paperTitle").value());
//                count++;
//                // use apis on Vertex to explore the node, below is the API link
//                // http://tinkerpop.apache.org/javadocs/3.2.3/full/org/apache/tinkerpop/gremlin/structure/Vertex.html#properties-java.lang.String...-
//            }
//            System.out.println("********** count ********** : " + count);


            // if you modify anything, always commit, for read only query, it is not needed
            // graphTransaction.commit();

            // close transaction when finishing using it
            graphTransaction.close();
            graph.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Reuse 'g' across the application
        // and close it on shut-down to close open connections with g.close()
    }
}
