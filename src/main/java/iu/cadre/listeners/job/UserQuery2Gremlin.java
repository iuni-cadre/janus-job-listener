package iu.cadre.listeners.job;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.logging.Level.INFO;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.count;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;
import static org.janusgraph.core.attribute.Text.textContains;
import static org.janusgraph.core.attribute.Text.textContainsFuzzy;

public class UserQuery2Gremlin {
    public static final String PAPER_FIELD = "Paper";
    private static final Logger LOG = LoggerFactory.getLogger(UserQuery2Gremlin.class);
    public static final String JOURNAL_FIELD = "JournalFixed";
    public static final String PUBLISHED_IN_FIELD = "PublishedInFixed";
    public static final String AUTHOR_FIELD = "Author";
    public static final String AUTHOR_OF_FIELD = "AuthorOf";

    public static Integer record_limit = 100000;

    public static TinkerGraph getSubGraphForQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (!query.DataSet().equals("mag"))
            throw new UnsupportedOperationException("Only MAG database is supported");

        LOG.info("Creating subgraph for the query");
        GraphTraversal<Vertex, Vertex> filterTraversal = traversal.V();

        List<Edge> edges = query.Edges();

        if (edges.isEmpty()) {
            Edge e = new Edge();
            e.source = PAPER_FIELD;
            e.target = AUTHOR_FIELD;
            e.relation = AUTHOR_OF_FIELD;
            edges.add(e);
        }
        Map<String, List<Object>> asLabelFilters = getASLabelFilters(query.Nodes());
        int count = 1;
        List<Object> allMatchClauses = new ArrayList<>();
        for (String vertexType : asLabelFilters.keySet()) {
            String label1 = "label" + count;
            count++;
            String label2 = "label" + count;
            List<Object> hasFilterListPerVertex = asLabelFilters.get(vertexType);
            allMatchClauses.addAll(hasFilterListPerVertex);

            for (Edge edge : edges) {
                LOG.info(edge.toString());
                if (edge.source.equals(PAPER_FIELD) && edge.target.equals(JOURNAL_FIELD)) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals(PAPER_FIELD) && edge.target.equals("ConferenceInstance")) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals(JOURNAL_FIELD) && edge.target.equals(PAPER_FIELD)) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals(AUTHOR_FIELD) && edge.target.equals(PAPER_FIELD)) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals("ConferenceInstance") && edge.target.equals(PAPER_FIELD)) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals(PAPER_FIELD) && edge.target.equals(AUTHOR_FIELD)) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals(PAPER_FIELD) && edge.target.equals(PAPER_FIELD)) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }
            }
        }

        GraphTraversal<?, ?>[] temp = new GraphTraversal[allMatchClauses.size()];
        for (int i = 0; i < allMatchClauses.size(); i++) {
            temp[i] = (GraphTraversal<?, ?>) allMatchClauses.get(i);
        }

        LOG.info(allMatchClauses.size() + " total clauses in query");

        // filterTraversal.match(temp).limit(record_limit).cap("sg").next().explain()
        TinkerGraph tg = (TinkerGraph) filterTraversal.match(temp).limit(record_limit).cap("sg").next();

        return tg;
    }

    public static Map<String, List<Object>> getASLabelFilters(List<Node> nodes) {
        Map<String, List<Object>> hasFilterMap = new LinkedHashMap<>();
        int i = 0;
        for (Node node : nodes) {
            String label = "label" + (i + 1);
            if (node.filters.isEmpty()) {
                LOG.warn("Node without filters requested, rejecting");
                continue;
            }
            String vertexType = node.type;
            List<Object> hasFilters = new ArrayList<>();
            for (Filter filterField : node.filters) {
                LOG.info(filterField.field + " = " + filterField.value);
                if (!filterField.field.equals("year") && !filterField.field.equals("doi")) {
                    GraphTraversal<Object, Object> asLabelWithFilters = __.as(label).has(vertexType, filterField.field, textContains(filterField.value));
                    hasFilters.add(asLabelWithFilters);
                } else {
                    GraphTraversal<Object, Object> asLabelWithFilters = __.as(label)
                            .has(vertexType, filterField.field, Integer.valueOf(filterField.value))
                            .values("paperId", "year");
                    hasFilters.add(asLabelWithFilters);
                }

                if (filterField.operator.equals("or")) {
                    int n = hasFilters.size() - 1;
                    GraphTraversal gt1 = (GraphTraversal<Object, Object>) hasFilters.get(n);
                    GraphTraversal gt2 = (GraphTraversal<Object, Object>) hasFilters.get(n - 1);
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

    static String edgeLabel(String source, String target) throws Exception {
        if (source.equals(PAPER_FIELD) && target.equals(AUTHOR_FIELD))
            return AUTHOR_OF_FIELD;

        if (source.equals(AUTHOR_FIELD) && target.equals(PAPER_FIELD))
            return AUTHOR_OF_FIELD;

        if (source.equals(PAPER_FIELD) && target.equals(JOURNAL_FIELD))
            return PUBLISHED_IN_FIELD;

        if (source.equals(JOURNAL_FIELD) && target.equals(PAPER_FIELD))
            return PUBLISHED_IN_FIELD;

        if (source.equals(AUTHOR_FIELD) && target.equals(JOURNAL_FIELD))
            return PUBLISHED_IN_FIELD;

        throw new Exception("No edge between " + source + " and " + target);
    }

    public static List getProjectionForQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        GraphTraversal t = traversal.V();

        if (query.Nodes().stream().anyMatch(n -> n.type.equals(JOURNAL_FIELD))) {
            return getProjectionForJournalQuery(t, query);
        }
        else if (query.Nodes().stream().anyMatch(n -> n.type.equals(AUTHOR_FIELD))) {
                return getProjectionForAuthorQuery(t, query);
        } else {
            return getProjectionForPaperQuery(t, query);
        }
    }

    private static List getProjectionForJournalQuery(GraphTraversal t, UserQuery query) throws Exception {
        List<Node> journalNodes = query.Nodes().stream().filter(n -> n.type.equals(JOURNAL_FIELD)).collect(Collectors.toList());
        for (Node journalNode : journalNodes) {
            Filter f = journalNode.filters.get(0);
            t = t.has(journalNode.type, f.field, textContains(f.value));
        }
        t = t.limit(100).both();
        List<Node> paperNodes = query.Nodes().stream().filter(n -> n.type.equals(PAPER_FIELD)).collect(Collectors.toList());
        for (Node paperNode : paperNodes) {
            Filter f = paperNode.filters.get(0);
            if (f.field.equals("year")) {
                t = t.has(paperNode.type, f.field, Integer.parseInt(f.value));
            } else if (f.field.equals("doi")) {
                t = t.has(paperNode.type, f.field, f.value);
            } else {
                t = t.has(paperNode.type, f.field, textContains(f.value));
            }
        }
        if (query.CSV().isEmpty()) {
            return t.valueMap().toList();
        }
        else {
            List<CSVOutput> nonAuthorNodes = query.CSV().stream().filter(v -> !v.vertexType.equals(AUTHOR_FIELD)).collect(Collectors.toList());
            String[] projections = nonAuthorNodes.stream().map(v -> v.vertexType + "_" + v.field).toArray(String[]::new);
            t = t.project(projections[0], ArrayUtils.subarray(projections, 1, projections.length));
            for (CSVOutput c : nonAuthorNodes) {
                if (c.vertexType.equals(PAPER_FIELD)) {
                    t = t.by(c.field);
                } else if (c.vertexType.equals(JOURNAL_FIELD)) {
                    t = t.by(__.both(edgeLabel(PAPER_FIELD, c.vertexType)).values(c.field).fold());
                }
            }
        }
        LOG.info("Query: " + t);
        return t.toList();
    }

    private static List getProjectionForPaperQuery(GraphTraversal t, UserQuery query) throws Exception {
        List<Node> paperNodes = query.Nodes().stream().filter(n -> n.type.equals(PAPER_FIELD)).collect(Collectors.toList());

        for (Node paperNode : paperNodes) {
            Filter f = paperNode.filters.get(0);
            if (f.field.equals("year") || f.field.equals("doi")) {
                t = t.has(paperNode.type, f.field, f.value);
            } else {
                t = t.has(paperNode.type, f.field, textContains(f.value));
            }
        }

        List<Node> otherNodes = query.Nodes().stream().filter(n -> !n.type.equals(PAPER_FIELD)).collect(Collectors.toList());
        for (Node otherNode : otherNodes) {
            Filter f = otherNode.filters.get(0);
            t = t.where(__.both(edgeLabel(PAPER_FIELD, otherNode.type)).has(otherNode.type, f.field, textContains(f.value)));
        }

        t = t.limit(record_limit).as("a");
        if (query.CSV().isEmpty()) {
            return t.valueMap().toList();
        } else {
            String[] projections = query.CSV().stream().map(v -> v.vertexType + "_" + v.field).toArray(String[]::new);
            t = t.project(projections[0], ArrayUtils.subarray(projections, 1, projections.length));
            for (CSVOutput c : query.CSV()) {
                if (c.vertexType.equals(PAPER_FIELD))
                    t = t.by(c.field);
                else {
                    t = t.by(__.both(edgeLabel(PAPER_FIELD, c.vertexType)).values(c.field).fold());
                }
            }
        }
        LOG.info("Query: " + t);
        return t.toList();
    }

    private static List getProjectionForAuthorQuery(GraphTraversal t, UserQuery query) throws Exception {
        List<Node> authorNodes = query.Nodes().stream().filter(n -> n.type.equals(AUTHOR_FIELD)).collect(Collectors.toList());

        for (Node authorNode : authorNodes) {
            Filter f = authorNode.filters.get(0);
            t = t.has(authorNode.type, f.field, textContains(f.value));
        }

        List<Node> otherNodes = query.Nodes().stream().filter(n -> !n.type.equals(AUTHOR_FIELD)).collect(Collectors.toList());
        for (Node otherNode : otherNodes) {
            Filter f = otherNode.filters.get(0);
            t = t.where(__.both(edgeLabel(AUTHOR_FIELD, otherNode.type)).has(otherNode.type, f.field, textContainsFuzzy(f.value)));
        }

        t = t.limit(record_limit).as("a");
        if (query.CSV().isEmpty()) {
            return t.valueMap().toList();
        } else {
            String[] projections = query.CSV().stream().map(v -> v.vertexType + "_" + v.field).toArray(String[]::new);
            t = t.project(projections[0], ArrayUtils.subarray(projections, 1, projections.length));
            for (CSVOutput c : query.CSV()) {
                if (c.vertexType.equals(AUTHOR_FIELD))
                    t = t.by(c.field);
                else if (c.vertexType.equals(PAPER_FIELD))
                    t = t.by(__.both(edgeLabel(AUTHOR_FIELD, c.vertexType)).values(c.field));
                else {
                    t = t.by(__.both(edgeLabel(AUTHOR_FIELD, c.vertexType)).values(c.field).fold());
                }
            }
        }
        LOG.info("Query: " + t);
        return t.toList();
    }
}
