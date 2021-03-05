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

import static iu.cadre.listeners.job.util.Constants.*;
import static org.janusgraph.core.attribute.Text.textContains;
import static org.janusgraph.core.attribute.Text.textContainsFuzzy;

public class UserQuery2Gremlin {
    public static final String PAPER_FIELD = "Paper";
    private static final Logger LOG = LoggerFactory.getLogger(UserQuery2Gremlin.class);
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

        if (source.equals(PAPER_FIELD) && target.equals(FIELD_OF_STUDY_FIELD))
            return BELONGS_TO_FIELD;

        if (source.equals(FIELD_OF_STUDY_FIELD) && target.equals(PAPER_FIELD))
            return BELONGS_TO_FIELD;

        if (source.equals(AUTHOR_FIELD) && target.equals(AFFILIATION_FIELD))
            return AFFILIATED__WITH_FIELD;

        if (source.equals(AFFILIATION_FIELD) && target.equals(AUTHOR_FIELD))
            return AFFILIATED__WITH_FIELD;

        if (source.equals(PATENT_FIELD) && target.equals(INVENTOR_FIELD))
            return INVENTOR_OF_FIELD;

        if (source.equals(INVENTOR_FIELD) && target.equals(PATENT_FIELD))
            return INVENTOR_OF_FIELD;

        if (source.equals(INVENTOR_FIELD) && target.equals(LOCATION_FIELD))
            return INVENTOR_LOCATED_IN_FIELD;

        if (source.equals(LOCATION_FIELD) && target.equals(INVENTOR_FIELD))
            return INVENTOR_LOCATED_IN_FIELD;

        if (source.equals(PATENT_FIELD) && target.equals(CPC_FIELD))
            return CPC_CATEGORY_OF_FIELD;

        if (source.equals(CPC_FIELD) && target.equals(PATENT_FIELD))
            return CPC_CATEGORY_OF_FIELD;

        if (source.equals(PATENT_FIELD) && target.equals(USPC_FIELD))
            return USPC_CATEGORY_OF_FIELD;

        if (source.equals(USPC_FIELD) && target.equals(PATENT_FIELD))
            return USPC_CATEGORY_OF_FIELD;

        if (source.equals(PATENT_FIELD) && target.equals(ASSIGNEE_FIELD))
            return ASSIGN_TO_FIELD;

        if (source.equals(ASSIGNEE_FIELD) && target.equals(PATENT_FIELD))
            return ASSIGN_TO_FIELD;

        throw new Exception("No edge between " + source + " and " + target);
    }

    public static List getPaperProjection(GraphTraversalSource traversal, List<Vertex> verticesList, UserQuery query) throws Exception
    {
        List<Map> gtList = new ArrayList<>();
        GraphTraversal t = null;
        if (query.CSV().isEmpty()) {
            for (Vertex v : verticesList) {
                t = traversal.V(v).valueMap();
                gtList.addAll(t.toList());
            }
        }else {
            String[] projections = query.CSV().stream().map(v -> v.vertexType + "_" + v.field).toArray(String[]::new);
            for (Vertex v : verticesList) {
                t = traversal.V(v).project(projections[0], ArrayUtils.subarray(projections, 1, projections.length));
                for (CSVOutput c : query.CSV()) {
                    if (!query.DataSet().equals("uspto")){
                        if (c.vertexType.equals(PAPER_FIELD))
                            t = t.by(__.coalesce(__.values(c.field), __.constant("")));
                        else if (c.vertexType.equals(AFFILIATION_FIELD)) {
//                        g.V(97581334600).project('Affiliation_displayName').by(both('AuthorOf').hasLabel('Author').bothE().bothV().hasLabel('Affiliation').properties('displayName').value()).fold()
                            t = t.by(__.both(AUTHOR_OF_FIELD).hasLabel(AUTHOR_FIELD).bothE().bothV().hasLabel(AFFILIATION_FIELD).values(c.field).fold());
                        }else{
                            t = t.by(__.both(edgeLabel(PAPER_FIELD, c.vertexType)).values(c.field).fold());
                        }
                    }else {
                        if (c.vertexType.equals(PATENT_FIELD))
                            t = t.by(__.coalesce(__.values(c.field), __.constant("")));
                        else if (c.vertexType.equals(LOCATION_FIELD)) {
                            t = t.by(__.both(INVENTOR_OF_FIELD).hasLabel(INVENTOR_FIELD).bothE().bothV().hasLabel(LOCATION_FIELD).values(c.field).fold());
                        }else{
                            t = t.by(__.both(edgeLabel(PATENT_FIELD, c.vertexType)).values(c.field).fold());
                        }
                    }
                }
                gtList.addAll(t.toList());
            }
        }
//        if (query.CSV().isEmpty()) {
//            return t.valueMap();
//        } else {
//            String[] projections = query.CSV().stream().map(v -> v.vertexType + "_" + v.field).toArray(String[]::new);
//            t = t.project(projections[0], ArrayUtils.subarray(projections, 1, projections.length));
//            for (CSVOutput c : query.CSV()) {
//                if (c.vertexType.equals(PAPER_FIELD))
//                    t = t.by(__.coalesce(__.values(c.field), __.constant("")));
//                else {
//                    t = t.by(__.both(edgeLabel(PAPER_FIELD, c.vertexType)).values(c.field).fold());
//                }
//            }
//        }

        return gtList;
    }

    public static List getPaperProjectionForNetwork(GraphTraversalSource traversal, List<Vertex> verticesList, UserQuery query) throws Exception
    {
        List<Map> gtList = new ArrayList<>();
        GraphTraversal t = null;
        boolean isCitationsGraph = query.RequiresCitationsGraph();
        boolean isReferencesGraph = query.RequiresReferencesGraph();

        if (isCitationsGraph && isReferencesGraph) {
            throw new UnsupportedOperationException("Citations graph and references graph are not supported in the same projection.");
        } else if (!isCitationsGraph && !isReferencesGraph) {
            throw new Exception("A citation or reference paper projection was not specified for requested network.");
        }

        for (Vertex v : verticesList) {
            if (query.DataSet().equals("mag")){
                if (isCitationsGraph) {
                    t = traversal.V(v).outE("References").project("From", "To").by(__.outV().values("paperId")).by(__.inV().values("paperId"));
                } else if (isReferencesGraph) {
                    t = traversal.V(v).inE("References").project("From", "To").by(__.outV().values("paperId")).by(__.inV().values("paperId"));
                }
            }else if(query.DataSet().equals("wos")) {
                if (isCitationsGraph) {
                    t = traversal.V(v).outE("References").project("From", "To").by(__.outV().values("wosId")).by(__.inV().values("wosId"));
                } else if (isReferencesGraph) {
                    t = traversal.V(v).inE("References").project("From", "To").by(__.outV().values("wosId")).by(__.inV().values("wosId"));
                }
            }else {
                if (isCitationsGraph) {
                    t = traversal.V(v).outE("Cites").project("From", "To").by(__.outV().values("patent_id")).by(__.inV().values("patent_id"));
                } else if (isReferencesGraph) {
                    t = traversal.V(v).inE("Cites").project("From", "To").by(__.outV().values("patent_id")).by(__.inV().values("patent_id"));
                }
            }
            gtList.addAll(t.toList());
        }
        return gtList;
    }

    private static GraphTraversal getPaperFilter(GraphTraversal t, UserQuery query, String edgeType) throws Exception {
        List<Node> paperNodes = query.Nodes().stream().filter(n -> n.type.equals(PAPER_FIELD)).collect(Collectors.toList());
        if (paperNodes.isEmpty())
        {
            /// even if we don't filter by paper, we still probably want to return a list of papers
            t = t.both(edgeLabel(edgeType, PAPER_FIELD));
        }
        else {
            t = t.both(edgeLabel(edgeType, PAPER_FIELD));
            for (Node paperNode : paperNodes) {
                for (Filter f : paperNode.filters) {
                    if (f.field.equals("year")) {
                        t = t.has(paperNode.type, f.field, Integer.parseInt(f.value));
                    } else if (f.field.equals("doi")) {
                        t = t.has(paperNode.type, f.field, f.value);
                    } else {
                        t = t.has(paperNode.type, f.field, support_fuzzy_queries ? textContainsFuzzy(f.value) : textContains(f.value));
                    }
                }
            }
        }

        return t;
    }

    private static GraphTraversal getPatentFilter(GraphTraversal t, UserQuery query, String edgeType) throws Exception {
        List<Node> patentNodes = query.Nodes().stream().filter(n -> n.type.equals(PATENT_FIELD)).collect(Collectors.toList());
        if (patentNodes.isEmpty())
        {
            /// even if we don't filter by paper, we still probably want to return a list of papers
            t = t.both(edgeLabel(edgeType, PATENT_FIELD));
        }
        else {
            if (edgeType.equals(LOCATION_FIELD)){
                for (Node patentNode : patentNodes) {
                    for (Filter f : patentNode.filters) {
//                        t = t.by(__.both(INVENTOR_OF_FIELD).hasLabel(INVENTOR_FIELD).bothE().bothV().has(LOCATION_FIELD, ));
                    }
                }
            }else {
                t = t.both(edgeLabel(edgeType, PATENT_FIELD));
                for (Node patentNode : patentNodes) {
                    for (Filter f : patentNode.filters) {
                        if (f.field.equals("number")) {
                            t = t.has(patentNode.type, f.field, f.value);
                        } else if (f.field.equals("year")) {
                            t = t.has(patentNode.type, f.field, f.value);
                        } else {
                            t = t.has(patentNode.type, f.field, support_fuzzy_queries ? textContainsFuzzy(f.value) : textContains(f.value));
                        }
                    }
                }
            }

        }

        return t;
    }

    public static List<Vertex> getMAGProjectionForQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (query.HasAbstractSearch())
            throw new UnsupportedOperationException("Search by abstract is not supported");

        if (query.Nodes().stream().anyMatch(n -> n.type.equals(JOURNAL_FIELD))) {
            return getProjectionForNonPaperQuery(traversal, query, JOURNAL_FIELD);
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(CONFERENCE_INSTANCE_FIELD))) {
            return getProjectionForNonPaperQuery(traversal, query, CONFERENCE_INSTANCE_FIELD);
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(AUTHOR_FIELD))) {
            return getProjectionForNonPaperQuery(traversal, query, AUTHOR_FIELD);
        } else {
            return getProjectionForPaperQueryMAG(traversal, query);
        }
    }

    public static List<Vertex> getWOSProjectionForQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (query.HasAbstractSearch())
            throw new UnsupportedOperationException("Search by abstract is not supported");
        return getProjectionForPaperQueryWOS(traversal, query);
    }

    public static List<Vertex> getUSPTOProjectionForQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (query.Nodes().stream().anyMatch(n -> n.type.equals(INVENTOR_FIELD))) {
            return getProjectionForNonPatentQuery(traversal, query, INVENTOR_FIELD);
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(LOCATION_FIELD))) {
            return getProjectionForNonPatentQuery(traversal, query, LOCATION_FIELD);
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(ASSIGNEE_FIELD))) {
            return getProjectionForNonPatentQuery(traversal, query, ASSIGNEE_FIELD);
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(USPC_FIELD))) {
            return getProjectionForNonPatentQuery(traversal, query, USPC_FIELD);
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(CPC_FIELD))) {
            return getProjectionForNonPatentQuery(traversal, query, CPC_FIELD);
        } else {
            return getProjectionForPaperQueryMAG(traversal, query);
        }
    }


    public static List<Vertex> getProjectionForNonPatentQuery(GraphTraversalSource traversal, UserQuery query, String nodeType) throws Exception {
        // Apply other filters
        List<Node> inventorNodes = query.Nodes().stream().filter(n -> n.type.equals(INVENTOR_FIELD)).collect(Collectors.toList());
        List<Node> cpcNodes = query.Nodes().stream().filter(n -> n.type.equals(CPC_FIELD)).collect(Collectors.toList());
        List<Node> uspcNodes = query.Nodes().stream().filter(n -> n.type.equals(USPC_FIELD)).collect(Collectors.toList());
        List<Node> locationNodes = query.Nodes().stream().filter(n -> n.type.equals(LOCATION_FIELD)).collect(Collectors.toList());
        List<Node> assigneeNodes = query.Nodes().stream().filter(n -> n.type.equals(ASSIGNEE_FIELD)).collect(Collectors.toList());

        GraphTraversal t1 = traversal.V();
        GraphTraversal t2 = traversal.V();
        GraphTraversal t3 = traversal.V();
        GraphTraversal t4 = traversal.V();
        GraphTraversal t5 = traversal.V();
        List<Vertex> nonPatentNodesList1 = new ArrayList<>();
        List<Vertex> nonPatentNodesList2 = new ArrayList<>();
        List<Vertex> nonPatentNodesList3 = new ArrayList<>();
        List<Vertex> nonPatentNodesList4 = new ArrayList<>();
        List<Vertex> nonPatentNodesList5 = new ArrayList<>();
        if (!inventorNodes.isEmpty()){
            for (Node n : inventorNodes) {
                for (Filter f : n.filters) {
                    t1 = t1.has(n.type, f.field, textContains(f.value));
                    nonPatentNodesList1 = t1.toList();
                }
            }
        }
        if (!cpcNodes.isEmpty()){
            for (Node n : cpcNodes) {
                for (Filter f : n.filters) {
                    t2 = t2.has(n.type, f.field, textContains(f.value));
                    nonPatentNodesList2 = t2.limit(record_limit*2).toList();
                }
            }
        }
        if (!uspcNodes.isEmpty()){
            for (Node n : uspcNodes) {
                for (Filter f : n.filters) {
                    t3 = t3.has(n.type, f.field, textContains(f.value));
                    nonPatentNodesList3 = t3.limit(record_limit*2).toList();
                }
            }
        }

        if (!locationNodes.isEmpty()){
            for (Node n : locationNodes) {
                for (Filter f : n.filters) {
                    t4 = t4.has(n.type, f.field, textContains(f.value));
                    nonPatentNodesList4 = t4.limit(record_limit*2).toList();
                }
            }
        }

        if (!assigneeNodes.isEmpty()){
            for (Node n : assigneeNodes) {
                for (Filter f : n.filters) {
                    t5 = t5.has(n.type, f.field, textContains(f.value));
                    nonPatentNodesList5 = t5.limit(record_limit*2).toList();
                }
            }
        }

        LOG.info("********* Non paper nodes returned ***********");
        LOG.info("********* size inventor nodes ***********" + nonPatentNodesList1.size());
        LOG.info("********* size cpc nodes *********** " + nonPatentNodesList2.size());
        LOG.info("********* size uspc nodes *********** " + nonPatentNodesList3.size());

        List<Vertex> patentFiltersWithInventor = new ArrayList<>();
        List<Vertex> patentFiltersWithCPC = new ArrayList<>();
        List<Vertex> patentFiltersWithUSPC = new ArrayList<>();
        List<Vertex> patentFiltersWithLocation = new ArrayList<>();
        List<Vertex> patentFiltersWithAssignee = new ArrayList<>();
        Set<Vertex> patentFilters = new HashSet<>();
        List<Vertex> patents = new ArrayList<>();
        int batchSize = 100;
        for (Vertex nonPatentVertex : nonPatentNodesList1){
            GraphTraversal gt  = getPatentFilter(traversal.V(nonPatentVertex), query, INVENTOR_FIELD);
            while (gt.hasNext()) {
                patentFiltersWithInventor.addAll(gt.next(batchSize));
            }
        }

        for (Vertex nonPatentVertex : nonPatentNodesList2){
            GraphTraversal gt  = getPatentFilter(traversal.V(nonPatentVertex), query, CPC_FIELD);
            while (gt.hasNext()) {
                patentFiltersWithCPC.addAll(gt.next(batchSize));
            }
        }

        for (Vertex nonPatentVertex : nonPatentNodesList3){
            GraphTraversal gt  = getPatentFilter(traversal.V(nonPatentVertex), query, USPC_FIELD);
            while (gt.hasNext()) {
                patentFiltersWithUSPC.addAll(gt.next(batchSize));
            }
        }

        for (Vertex nonPatentVertex : nonPatentNodesList4){
            GraphTraversal gt  = getPatentFilter(traversal.V(nonPatentVertex), query, LOCATION_FIELD);
            while (gt.hasNext()) {
                patentFiltersWithUSPC.addAll(gt.next(batchSize));
            }
        }

        for (Vertex nonPatentVertex : nonPatentNodesList5){
            GraphTraversal gt  = getPatentFilter(traversal.V(nonPatentVertex), query, ASSIGNEE_FIELD);
            while (gt.hasNext()) {
                patentFiltersWithAssignee.addAll(gt.next(batchSize));
            }
        }


        LOG.info("Size authorPaperFilters " + patentFiltersWithInventor.size());
        LOG.info("Size journalPaperFilters " + patentFiltersWithCPC.size());
        LOG.info("Size confPaperFilters " + patentFiltersWithUSPC.size());

        Set<Vertex> intersection1 = intersection(patentFiltersWithInventor, patentFiltersWithCPC);
        patentFilters = intersection(patentFiltersWithUSPC, new ArrayList<>(intersection1));

        LOG.info("size " + patentFilters.size());
        for (Vertex paper : patentFilters){
            GraphTraversal gt  = traversal.V(paper);

            if (query.RequiresCitationsGraph()) {
                gt = gt.outE("Citation").bothV().dedup();
            } else if (query.RequiresReferencesGraph()) {
                gt = gt.inE("Citation").bothV().dedup();
            }

            while (gt.hasNext()) {
                if (patents.size() < (record_limit - 100)){
                    patents.addAll(gt.next(batchSize));
                }
                else {
                    break;
                }
            }
            if (patents.size() >= record_limit - 100)
                break;

        }
        LOG.info("********* Patents returned **********");
        return patents;
    }


    public static List<Vertex> getProjectionForNonPaperQuery(GraphTraversalSource traversal, UserQuery query, String nodeType) throws Exception {
        // Apply other filters
        List<Node> authorNodes = query.Nodes().stream().filter(n -> n.type.equals(AUTHOR_FIELD)).collect(Collectors.toList());
        List<Node> journalNodes = query.Nodes().stream().filter(n -> n.type.equals(JOURNAL_FIELD)).collect(Collectors.toList());
        List<Node> confInstanceNodes = query.Nodes().stream().filter(n -> n.type.equals(CONFERENCE_INSTANCE_FIELD)).collect(Collectors.toList());

        GraphTraversal t1 = traversal.V();
        GraphTraversal t2 = traversal.V();
        GraphTraversal t3 = traversal.V();
        List<Vertex> nonPaperNodesList1 = new ArrayList<>();
        List<Vertex> nonPaperNodesList2 = new ArrayList<>();
        List<Vertex> nonPaperNodesList3 = new ArrayList<>();
        if (!authorNodes.isEmpty()){
            for (Node n : authorNodes) {
                for (Filter f : n.filters) {
                    t1 = t1.has(n.type, f.field, textContains(f.value));
                    if (nodeType.equals(AUTHOR_FIELD)){
                        nonPaperNodesList1 = t1.limit(record_limit*2).toList();
                    }else {
                        nonPaperNodesList1 = t1.toList();
                    }
                }
            }
        }
        if (!journalNodes.isEmpty()){
            for (Node n : journalNodes) {
                for (Filter f : n.filters) {
                    t2 = t2.has(n.type, f.field, textContains(f.value));
                    nonPaperNodesList2 = t2.limit(record_limit*2).toList();
                }
            }
        }
        if (!confInstanceNodes.isEmpty()){
            for (Node n : confInstanceNodes) {
                for (Filter f : n.filters) {
                    t3 = t3.has(n.type, f.field, textContains(f.value));
                    nonPaperNodesList3 = t3.limit(record_limit*2).toList();
                }
            }
        }

        LOG.info("********* Non paper nodes returned ***********");
        LOG.info("********* size authornodes ***********" + nonPaperNodesList1.size());
        LOG.info("********* size journalnodes *********** " + nonPaperNodesList2.size());
        LOG.info("********* size confInstanceNodes *********** " + nonPaperNodesList3.size());

        List<Vertex> paperFiltersWithAuthor = new ArrayList<>();
        List<Vertex> paperFiltersWithJournal = new ArrayList<>();
        List<Vertex> paperFiltersWithConfInst = new ArrayList<>();
        Set<Vertex> paperFilters = new HashSet<>();
        List<Vertex> papers = new ArrayList<>();
        int batchSize = 100;
        for (Vertex nonPaperVertex : nonPaperNodesList1){
            GraphTraversal gt  = getPaperFilter(traversal.V(nonPaperVertex), query, AUTHOR_FIELD);
            while (gt.hasNext()) {
                paperFiltersWithAuthor.addAll(gt.next(batchSize));
            }
        }

        for (Vertex nonPaperVertex : nonPaperNodesList2){
            GraphTraversal gt  = getPaperFilter(traversal.V(nonPaperVertex), query, JOURNAL_FIELD);
            while (gt.hasNext()) {
                paperFiltersWithJournal.addAll(gt.next(batchSize));
            }
        }

        for (Vertex nonPaperVertex : nonPaperNodesList3){
            GraphTraversal gt  = getPaperFilter(traversal.V(nonPaperVertex), query, CONFERENCE_INSTANCE_FIELD);
            while (gt.hasNext()) {
                paperFiltersWithConfInst.addAll(gt.next(batchSize));
            }
        }

        LOG.info("Size authorPaperFilters " + paperFiltersWithAuthor.size());
        LOG.info("Size journalPaperFilters " + paperFiltersWithJournal.size());
        LOG.info("Size confPaperFilters " + paperFiltersWithConfInst.size());

        Set<Vertex> intersection1 = intersection(paperFiltersWithAuthor, paperFiltersWithJournal);
        paperFilters = intersection(paperFiltersWithConfInst, new ArrayList<>(intersection1));

        LOG.info("size " + paperFilters.size());
        for (Vertex paper : paperFilters){
            GraphTraversal gt  = traversal.V(paper);

            if (query.RequiresCitationsGraph()) {
                gt = gt.outE("References").bothV().dedup();
            } else if (query.RequiresReferencesGraph()) {
                gt = gt.inE("References").bothV().dedup();
            }

            while (gt.hasNext()) {
                if (papers.size() < (record_limit - 100)){
                    papers.addAll(gt.next(batchSize));
                }
                else {
                    break;
                }
            }
            if (papers.size() >= record_limit - 100)
                break;

        }
        LOG.info("********* Papers returned **********");
        return papers;
    }


    public static <T> Set<T> intersection(List<T> list1, List<T> list2) {
        Set<T> list = new HashSet<>();
        if (list1.isEmpty()){
            return new HashSet<>(list2);
        }else if (list2.isEmpty()){
            return new HashSet<>(list1);
        }

        Set<T> first = new HashSet<>(list1);

        for (T t : list2) {
            if(first.contains(t)) {
                list.add(t);
            }
        }
        return list;
    }

    private static List<Vertex> getProjectionForPaperQueryMAG(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (query.Nodes().stream().anyMatch(n -> !n.type.equals(PAPER_FIELD)))
            throw new UnexpectedException("Can't filter non-paper nodes");
        GraphTraversal t = traversal.V();
        for (Node paperNode : query.Nodes()) {
//          Get all the papers with one filters first
            if (paperNode.filters.stream().anyMatch(f -> f.field.equals("doi"))){
                for (Filter f : paperNode.filters) {
                    if (f.field.equals("doi")) {
                        t = t.has(paperNode.type, f.field, f.value);
                    }
                }
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("paperTitle"))){
                for (Filter f : paperNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("paperTitle")) {
                        t = t.has(paperNode.type, f.field, textContains(f.value));
                    }
                }
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("year"))){
                for (Filter f : paperNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("year")) {
                        t = t.has(paperNode.type, f.field, f.value);
                    }
                }
            }
        }
        LOG.info("Query: " + t);
        List<Vertex> filteredPapers = new ArrayList<>();
        int batchSize = 100;
        while (t.hasNext()) {
            Vertex next = (Vertex) t.next();
            GraphTraversal gt = traversal.V(next);
            List<Node> paperNodes = query.Nodes().stream().filter(n -> n.type.equals(PAPER_FIELD)).collect(Collectors.toList());
            for (Node paperNode : paperNodes) {
                for (Filter f : paperNode.filters) {
                    if (f.field.equals("year")) {
                        gt = gt.has(paperNode.type, f.field, Integer.parseInt(f.value));
                    } else if (f.field.equals("doi")) {
                        gt = gt.has(paperNode.type, f.field, f.value);
                    } else {
                        gt = gt.has(paperNode.type, f.field, support_fuzzy_queries ? textContainsFuzzy(f.value) : textContains(f.value));
                    }
                }
            }

            if (query.RequiresCitationsGraph()) {
                gt = gt.outE("References").bothV().dedup();
            } else if (query.RequiresReferencesGraph()) {
                gt = gt.inE("References").bothV().dedup();
            }

            if (filteredPapers.size() < (record_limit)){
                while (gt.hasNext()) {
                    filteredPapers.addAll(gt.next(batchSize));
                }
            }
            else
                break;
        }
        LOG.info("size ****** " + filteredPapers.size());

        return filteredPapers;
    }

    private static List<Vertex> getProjectionForPatentQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (query.Nodes().stream().anyMatch(n -> !n.type.equals(PATENT_FIELD)))
            throw new UnexpectedException("Can't filter non-patent nodes");
        GraphTraversal t = traversal.V();
        for (Node patentNode : query.Nodes()) {
//          Get all the papers with one filters first
            if (patentNode.filters.stream().anyMatch(f -> f.field.equals("number"))){
                for (Filter f : patentNode.filters) {
                    if (f.field.equals("number")) {
                        t = t.has(patentNode.type, f.field, f.value);
                    }
                }
            }else if (patentNode.filters.stream().anyMatch(f -> f.field.equals("title"))){
                for (Filter f : patentNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("title")) {
                        t = t.has(patentNode.type, f.field, textContains(f.value));
                    }
                }
            }else if (patentNode.filters.stream().anyMatch(f -> f.field.equals("year"))){
                for (Filter f : patentNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("year")) {
                        t = t.has(patentNode.type, f.field, Integer.valueOf(f.value));
                    }
                }
            }
        }
        LOG.info("Query: " + t);
        List<Vertex> filteredPatents = new ArrayList<>();
        int batchSize = 100;
        while (t.hasNext()) {
            Vertex next = (Vertex) t.next();
            GraphTraversal gt = traversal.V(next);
            List<Node> patentNodes = query.Nodes().stream().filter(n -> n.type.equals(PATENT_FIELD)).collect(Collectors.toList());
            for (Node patentNode : patentNodes) {
                for (Filter f : patentNode.filters) {
                    if (f.field.equals("year")) {
                        gt = gt.has(patentNode.type, f.field, Integer.parseInt(f.value));
                    } else if (f.field.equals("number")) {
                        gt = gt.has(patentNode.type, f.field, f.value);
                    } else {
                        gt = gt.has(patentNode.type, f.field, support_fuzzy_queries ? textContainsFuzzy(f.value) : textContains(f.value));
                    }
                }
            }

            if (query.RequiresCitationsGraph()) {
                gt = gt.outE("Cites").bothV().dedup();
            } else if (query.RequiresReferencesGraph()) {
                gt = gt.inE("Cites").bothV().dedup();
            }

            if (filteredPatents.size() < (record_limit)){
                while (gt.hasNext()) {
                    filteredPatents.addAll(gt.next(batchSize));
                }
            }
            else
                break;
        }
        LOG.info("size ****** " + filteredPatents.size());

        return filteredPatents;
    }

    private static List<Vertex> getProjectionForPaperQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (query.Nodes().stream().anyMatch(n -> !n.type.equals(PAPER_FIELD)))
            throw new UnexpectedException("Can't filter non-paper nodes here");
        GraphTraversal t = traversal.V();
        for (Node paperNode : query.Nodes()) {
            for (Filter f : paperNode.filters) {
                LOG.info("******** " + paperNode.filters.size());
                if (query.DataSet().equals("mag")){
                    if (f.field.equals("year") || f.field.equals("doi")) {
                        t = t.has(paperNode.type, f.field, f.value);
                    } else {
                        t = t.has(paperNode.type, f.field, support_fuzzy_queries ? textContainsFuzzy(f.value) : textContains(f.value));
                    }
                }else {
                    LOG.info(f.field);
                    if (f.field.equals("publicationYear") || f.field.equals("DOI")) {
                        t = t.has(paperNode.type, f.field, f.value);
                    } else {
                        t = t.has(paperNode.type, f.field, textContains(f.value));
                    }
                }

                if (query.RequiresGraph()){
                    t = t.outE("References").bothV().dedup();
                }
            }
        }
        t = t.limit(record_limit);
        LOG.info("Query: " + t);
        return t.toList();
    }

    private static List<Vertex> getProjectionForPaperQueryWOS(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (query.Nodes().stream().anyMatch(n -> !n.type.equals(PAPER_FIELD)))
            throw new UnexpectedException("Can't filter non-paper nodes");
        GraphTraversal t = traversal.V();
        for (Node paperNode : query.Nodes()) {
//          Get all the papers with one filters first
            if (paperNode.filters.stream().anyMatch(f -> f.field.equals("DOI"))){
                for (Filter f : paperNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("DOI")) {
                        t = t.has(paperNode.type, f.field, f.value);
                    }
                }
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("articleTitle"))){
                for (Filter f : paperNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("articleTitle")) {
                        t = t.has(paperNode.type, f.field, textContains(f.value));
                    }
                }
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("sourceTitle"))){
                for (Filter f : paperNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("sourceTitle")) {
                        t = t.has(paperNode.type, f.field, textContains(f.value));
                    }
                }
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("authorFullNames"))){
                for (Filter f : paperNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("authorFullNames")) {
                        t = t.has(paperNode.type, f.field, textContains(f.value));
                    }
                }
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("publicationYear"))){
                for (Filter f : paperNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("publicationYear")) {
                        t = t.has(paperNode.type, f.field, Integer.valueOf(f.value));
                    }
                }
            }
        }
        LOG.info("Query: " + t);
        List<Vertex> filteredPapers = new ArrayList<>();
        int batchSize = 100;
        while (t.hasNext()) {
            Vertex next = (Vertex) t.next();
            GraphTraversal gt = traversal.V(next);
            for (Node paperNode : query.Nodes()) {
                for (Filter f : paperNode.filters) {
                    if (f.field.equals("year")) {
                        gt  = gt.has(paperNode.type, f.field, Integer.valueOf(f.value));
                    } else {
                        gt  = gt.has(paperNode.type, f.field, textContains(f.value));
                    }
                }
            }

            if (query.RequiresCitationsGraph()) {
                gt = gt.outE("References").bothV().dedup();
            } else if (query.RequiresReferencesGraph()) {
                gt = gt.inE("References").bothV().dedup();
            }

            if (filteredPapers.size() < (record_limit)){
                while (gt.hasNext()) {
                    filteredPapers.addAll(gt.next(batchSize));
                }
            }
            else
                break;
        }
        return filteredPapers;
    }
}
