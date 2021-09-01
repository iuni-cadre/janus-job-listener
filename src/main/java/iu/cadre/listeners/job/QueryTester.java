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

import java.util.*;

import static org.janusgraph.core.attribute.Text.textContains;
import static org.janusgraph.core.attribute.Text.textContainsFuzzy;

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

        long count1 = traversal.V().has("JournalFixed", "displayName", textContains("nature")).count().next();
        LOG.info(count1);
        long count2 = traversal.V().has("Paper", "year", 2000).count().next();
        LOG.info(count2);
        long count3 = traversal.V().has("Paper", "paperTitle", textContains("social")).count().next();
        LOG.info(count3);
        List<Vertex> journals = traversal.V().has("JournalFixed", "displayName", textContains("nature")).toList();
        long time2 = System.currentTimeMillis();
        LOG.info("Journals with nature returned");
        long timeForJournals = time2 - time1;
        LOG.info("Time to return journals : " + timeForJournals);
        List<Vertex> papers = new ArrayList<>();
        int batchSize = 100;
        Map<String, String> filters = new HashMap<>();
        filters.put("year", "2013");
        filters.put("paperTitle", "climate");
        for (Vertex journal : journals) {
            // loop over each journal node and run the following query
            GraphTraversal<Vertex, Vertex> t = traversal.V(journal);
            t = t.both("PublishedIn");
            for (String filtername : filters.keySet()){
                if (filtername.equals("year")) {
                    t = t.has("Paper", filtername, Integer.parseInt(filters.get(filtername)));
                } else {
                    t = t.has("Paper", filtername, textContainsFuzzy(filters.get(filtername)));
                }
            }
            LOG.info("Query " + t);

            while (t.hasNext()) {
                papers.addAll(t.next(batchSize));
            }
        }

        long time3 = System.currentTimeMillis();
        long timeForAll = time3 - time1;
        LOG.info("Time to return all : " + timeForAll);
        LOG.info("Paper Count : " + papers.size());
    }
}
