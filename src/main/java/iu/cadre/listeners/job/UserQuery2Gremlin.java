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
import java.rmi.UnexpectedException;

import static org.janusgraph.core.attribute.Text.textContains;
import static org.janusgraph.core.attribute.Text.textContainsFuzzy;

public class UserQuery2Gremlin {
    public static final String PAPER_FIELD = "Paper";
    private static final Logger LOG = LoggerFactory.getLogger(UserQuery2Gremlin.class);
    public static final String JOURNAL_FIELD = "JournalFixed";
    public static final String PUBLISHED_IN_FIELD = "PublishedInFixed";
    public static final String AUTHOR_FIELD = "Author";
    public static final String AUTHOR_OF_FIELD = "AuthorOf";
    public static final String CONFERENCE_INSTANCE_FIELD = "ConferenceInstance";
    public static final String PRESENTED_AT_FIELD = "PresentedAt";

    public static Integer record_limit = 100000;
    public static Boolean support_fuzzy_queries = true;

    public static TinkerGraph getSubGraphForQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (!query.DataSet().equals("mag"))
            throw new UnsupportedOperationException("Only MAG database is supported");

        LOG.info("Creating subgraph for the query");
        GraphTraversal<Vertex, Vertex> filterTraversal = traversal.V();


        List<Edge> edges = query.Edges();
        LOG.info("egde size " + edges.size());

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
        String lastLable1 = null;
        String lastLable2 = null;
        for (String vertexType : asLabelFilters.keySet()) {
            String label1 = "label" + count;
            count++;
            String label2 = "label" + count;
            lastLable1 = label1;
            lastLable2 = label2;
            LOG.info(vertexType);
            List<Object> hasFilterListPerVertex = asLabelFilters.get(vertexType);
            LOG.info("has filter count for vertex : " + vertexType + " is : " + hasFilterListPerVertex.size());
            allMatchClauses.addAll(hasFilterListPerVertex);

            for (Edge edge : edges) {
                LOG.info(edge.toString());
                if (edge.source.equals(vertexType)) {
                    if (edge.source.equals(PAPER_FIELD) && edge.target.equals(JOURNAL_FIELD)) {
                        LOG.info("paper -> journal");
                        GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                        allMatchClauses.add(nextAsLabel);
                    } else if (edge.source.equals(PAPER_FIELD) && edge.target.equals("ConferenceInstance")) {
                        LOG.info("paper -> confInstance");
                        GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                        allMatchClauses.add(nextAsLabel);
                    } else if (edge.source.equals(JOURNAL_FIELD) && edge.target.equals(PAPER_FIELD)) {
                        LOG.info("journal -> paper");
                        GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                        allMatchClauses.add(nextAsLabel);
                    } else if (edge.source.equals(AUTHOR_FIELD) && edge.target.equals(PAPER_FIELD)) {
                        LOG.info("author -> paper");
                        GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                        allMatchClauses.add(nextAsLabel);
                    } else if (edge.source.equals("ConferenceInstance") && edge.target.equals(PAPER_FIELD)) {
                        LOG.info("confInstance -> paper");
                        GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                        allMatchClauses.add(nextAsLabel);
                    } else if (edge.source.equals(PAPER_FIELD) && edge.target.equals(AUTHOR_FIELD)) {
                        LOG.info("paper -> author");
                        GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                        allMatchClauses.add(nextAsLabel);
                    }
                }
//                else if (edge.source.equals(PAPER_FIELD) && edge.target.equals(PAPER_FIELD)) {
//                    LOG.info("paper -> paper");
//                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
//                    allMatchClauses.add(nextAsLabel);
//                }
            }
        }

        for (Edge edge : edges) {
            if (edge.source.equals(PAPER_FIELD) && edge.target.equals(PAPER_FIELD)) {
                LOG.info("paper -> paper");
                LOG.info(lastLable1 + " " + lastLable2);
                GraphTraversal<Object, Vertex> nextAsLabel = __.as(lastLable1).inE(edge.relation).subgraph("sg").outV().as(lastLable2).limit(record_limit);
                allMatchClauses.add(nextAsLabel);
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
                            .has(vertexType, filterField.field, Integer.valueOf(filterField.value));
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

        if (source.equals(PAPER_FIELD) && target.equals(CONFERENCE_INSTANCE_FIELD))
            return PRESENTED_AT_FIELD;

        if (source.equals(CONFERENCE_INSTANCE_FIELD) && target.equals(PAPER_FIELD))
            return PRESENTED_AT_FIELD;

        throw new Exception("No edge between " + source + " and " + target);
    }

    public static GraphTraversal getPaperProjection(GraphTraversal t, UserQuery query) throws Exception
    {
        if (query.CSV().isEmpty()) {
            return t.valueMap();
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

        return t;
    }

    public static GraphTraversal getPaperProjectionForNetwork(GraphTraversal t, UserQuery query) throws Exception
    {
        t = t.project("From", "To").by(__.outV().id()).by(__.inV().id());
        return t;
    }

    private static GraphTraversal getPaperFilter(GraphTraversal t, UserQuery query, String edgeType) throws Exception {
        List<Node> paperNodes = query.Nodes().stream().filter(n -> n.type.equals(PAPER_FIELD)).collect(Collectors.toList());
        if (paperNodes.isEmpty())
        {
            /// even if we don't filter by paper, we still probably want to return a list of papers
            t = t.both(edgeLabel(edgeType, PAPER_FIELD));
        }
        else {
            for (Node paperNode : paperNodes) {
                for (Filter f : paperNode.filters) {
                    if (f.field.equals("year")) {
                        t = t.both(edgeLabel(edgeType, PAPER_FIELD)).has(paperNode.type, f.field, Integer.parseInt(f.value));
                    } else if (f.field.equals("doi")) {
                        t = t.both(edgeLabel(edgeType, PAPER_FIELD)).has(paperNode.type, f.field, f.value);
                    } else {
                        t = t.both(edgeLabel(edgeType, PAPER_FIELD)).has(paperNode.type, f.field, support_fuzzy_queries ? textContainsFuzzy(f.value) : textContains(f.value));
                    }
                }
            }
        }

        return t;
    }

    public static GraphTraversal getProjectionForQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (query.HasAbstractSearch())
            throw new UnsupportedOperationException("Search by abstract is not supported");
        GraphTraversal t = traversal.V();

        if (query.Nodes().stream().anyMatch(n -> n.type.equals(JOURNAL_FIELD))) {
            return getProjectionForNonPaperQuery(t, query, JOURNAL_FIELD);
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(AUTHOR_FIELD))) {
            return getProjectionForNonPaperQuery(t, query, AUTHOR_FIELD);
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(CONFERENCE_INSTANCE_FIELD))) {
            return getProjectionForNonPaperQuery(t, query, CONFERENCE_INSTANCE_FIELD);
        } else {
            return getProjectionForPaperQuery(t, query);
        }
    }

    public static GraphTraversal getProjectionForNonPaperQuery(GraphTraversal t, UserQuery query, String nodeType) throws Exception {
        List<Node> nonPaperNodes = query.Nodes().stream().filter(n -> n.type.equals(nodeType)).collect(Collectors.toList());
        for (Node n : nonPaperNodes) {
            for (Filter f : n.filters) {
                t = t.has(n.type, f.field, textContains(f.value));
            }
        }

        t = getPaperFilter(t, query, nodeType);

        LOG.info("Query: " + t);
        return t;
    }

    private static GraphTraversal getProjectionForPaperQuery(GraphTraversal t, UserQuery query) throws Exception {
        if (query.Nodes().stream().anyMatch(n -> !n.type.equals(PAPER_FIELD)))
            throw new UnexpectedException("Can't filter non-paper nodes here");

        for (Node paperNode : query.Nodes()) {

            for (Filter f : paperNode.filters) {
                if (f.field.equals("year") || f.field.equals("doi")) {
                    t = t.has(paperNode.type, f.field, f.value);
                } else {
                    t = t.has(paperNode.type, f.field, support_fuzzy_queries ? textContainsFuzzy(f.value) : textContains(f.value));
                }
            }
        }
        LOG.info("Query: " + t);
        return t;
    }
}
