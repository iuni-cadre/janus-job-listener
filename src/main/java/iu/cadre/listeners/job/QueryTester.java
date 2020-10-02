package iu.cadre.listeners.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.ArrayList;
import java.util.List;

import static org.janusgraph.core.attribute.Text.textContains;

public class QueryTester {
    protected static final Log LOG = LogFactory.getLog(QueryTester.class);

    public static void main(String[] args) {
        LOG.info("****************************");
        long time1 = System.currentTimeMillis();
        if (null == args || args.length != 1) {
            System.err.println(
                    "Usage: JanusGraphConnSample <janusgraph-config-file>");
            System.exit(1);
        }

        final JanusGraph graph = JanusGraphFactory.open(args[0]);

        StandardJanusGraph standardGraph = (StandardJanusGraph) graph;

        // get graph management
        JanusGraphManagement mgmt = standardGraph.openManagement();
        JanusGraphTransaction graphTransaction = graph.newTransaction();

        GraphTraversalSource traversal = graphTransaction.traversal();

        List<Vertex> journals = traversal.V().has("JournalFixed", "displayName", textContains("nature")).toList();
        long time2 = System.currentTimeMillis();
        LOG.info("Journals with nature returned");
        long timeForJournals = time2 - time1;
        LOG.info("Time to return journals : " + timeForJournals);
        List<Vertex> papers = new ArrayList<>();
        int batchSize = 100;
        for (Vertex journal : journals) {
            // loop over each journal node and run the following query
            GraphTraversal<Vertex, Vertex> gt = traversal.V(journal).both("PublishedInFixed").has("year", 2013).has("paperTitle", textContains("climate")).limit(10000);
            while (gt.hasNext()) {
                papers.addAll(gt.next(batchSize));
            }
        }

        long time3 = System.currentTimeMillis();
        long timeForAll = time3 - time1;
        LOG.info("Time to return all : " + timeForAll);
        LOG.info("Paper Count : " + papers.size());
    }
}
