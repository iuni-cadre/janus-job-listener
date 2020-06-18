package iu.cadre.listeners.job;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.janusgraph.core.attribute.Text.textContains;

public class UserQuery2Gremlin {
    private static final Logger LOG = LoggerFactory.getLogger(UserQuery2Gremlin.class);

    public static TinkerGraph getSubGraphForQuery(GraphTraversalSource traversal, UserQuery query, List<String> outputFields){
        if (!query.DataSet().equals("mag"))
            throw new UnsupportedOperationException("Only MAG database is supported");

        LOG.info("Creating subgraph for the query");
        GraphTraversal<Vertex, Vertex> filterTraversal = traversal.V();
        List<Edge> edges = query.Edges();
        Map<String, List<Object>> asLabelFilters = getASLabelFilters(query.Nodes());
        int count = 1;
        List<Object> allMatchClauses = new ArrayList<>();
        for (String vertexType : asLabelFilters.keySet()){
            String label1 = "label" + count;
            count++;
            String label2 = "label" + count;
            List<Object> hasFilterListPerVertex = asLabelFilters.get(vertexType);
            allMatchClauses.addAll(hasFilterListPerVertex);

            ListIterator<Edge> litr=edges.listIterator();
            while(litr.hasNext()){
                Edge edge = litr.next();
                if (edge.source.equals("Paper") && edge.target.equals("JournalFixed")){
	                LOG.info("**** PAPER JOURNAL ****");
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (edge.source.equals("Paper") && edge.target.equals("ConferenceInstance")){
                    LOG.info("**** PAPER ConferenceInstance ****");
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (edge.source.equals("JournalFixed") && edge.target.equals("Paper")){
                    LOG.info("**** JOURNAL PAPER ****");
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (edge.source.equals("Author") && edge.target.equals("Paper")){
                    LOG.info("**** AUTHOR PAPER ****");
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (edge.source.equals("ConferenceInstance") && edge.target.equals("Paper")){
                    LOG.info("**** CONFERENCEINSTANCE PAPER ****");
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (edge.source.equals("Paper") && edge.target.equals("Author")){
                    LOG.info("**** PAPER AUTHOR ****");
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (edge.source.equals("Paper") && edge.target.equals("Paper")){
                    LOG.info("**** PAPER PAPER ****");
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }
            }
        }
        GraphTraversal<?, ?> temp[] = new GraphTraversal[allMatchClauses.size()];
        for (int i = 0; i < allMatchClauses.size(); i++) {
            temp[i] = (GraphTraversal<?, ?>) allMatchClauses.get(i);
        }
        LOG.info("****** COUNT ***** " + allMatchClauses.size());
        TinkerGraph tg = (TinkerGraph)filterTraversal.match(temp).cap("sg").next();
        return tg;
    }

    public static Map<String, List<Object>> getASLabelFilters(List<Node> nodes){
        Map<String, List<Object>> hasFilterMap = new LinkedHashMap<>();
        ListIterator<Node> litr=nodes.listIterator();
        int i = 0;
        while(litr.hasNext()){
            String label = "label" + (i+1);
            Node node = litr.next();
            String vertexType = node.type;
            List<Object> hasFilters = new ArrayList<>();
            ListIterator<Filter> fitr=node.filters.listIterator();
            while(fitr.hasNext()){
                Filter filterField = fitr.next();
                LOG.info(filterField.field);
                LOG.info(filterField.value);
                if (!filterField.field.equals("year") && !filterField.field.equals("doi")){
                    GraphTraversal<Object, Object> asLabelWithFilters = __.as(label).has(vertexType, filterField.field, textContains(filterField.value));
                    hasFilters.add(asLabelWithFilters);
                }else {
                    GraphTraversal<Object, Object> asLabelWithFilters = __.as(label).has(vertexType, filterField.field, Integer.valueOf(filterField.value));
                    hasFilters.add(asLabelWithFilters);
                }

                if (filterField.operator.equals("or"))
                {
                    int n = hasFilters.size() - 1;
                    GraphTraversal<Object, Object> gt1 = (GraphTraversal<Object, Object>)hasFilters.get(n);
                    GraphTraversal<Object, Object> gt2 = (GraphTraversal<Object, Object>)hasFilters.get(n-1);
                    hasFilters.remove(gt1);
                    hasFilters.remove(gt2);
                    hasFilters.add(__.as(label).or(gt1, gt2));
                }
            }
            hasFilterMap.put(vertexType, hasFilters);
            i++;
        }
        return hasFilterMap;
    }


}
