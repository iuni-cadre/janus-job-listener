package iu.cadre.listeners.job;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
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
    private static String QUERY_PAPER_HEADER = "isQueryPaper";
    private static int maxBatchSize = 100;

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

    public static List getPaperProjection(GraphTraversalSource traversal, UserQuery query, List<List<Vertex>> levels) throws Exception
    {
        List<Map> gtList = new ArrayList<>();
        GraphTraversal t = null;

        if (query.CSV().isEmpty()) {
            for (List<Vertex> verticesList : levels) {
                for (Vertex v : verticesList) {
                    t = traversal.V(v).valueMap();
                    gtList.addAll(t.toList());
                }
            }
        } else {
            String[] projections = query.CSV().stream().map(v -> v.vertexType + "_" + v.field).toArray(String[]::new);

            for (List<Vertex> verticesList : levels) {
                for (Vertex v : verticesList) {
                    t = traversal.V(v).project(projections[0], ArrayUtils.subarray(projections, 1, projections.length));
                    for (CSVOutput c : query.CSV()) {
                        if (!query.DataSet().equals("uspto")) {
                            if (c.vertexType.equals(PAPER_FIELD))
                                t = t.by(__.coalesce(__.values(c.field), __.constant("")));
                            else if (c.vertexType.equals(AFFILIATION_FIELD)) {
//                              g.V(97581334600).project('Affiliation_displayName').by(both('AuthorOf').hasLabel('Author').bothE().bothV().hasLabel('Affiliation').properties('displayName').value()).fold()
                                t = t.by(__.both(AUTHOR_OF_FIELD).hasLabel(AUTHOR_FIELD).bothE().bothV().hasLabel(AFFILIATION_FIELD).values(c.field).fold());
                            } else {
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
        }

        // Mark the query papers.  All query papers were
        // are at the front of the list, so add the mark there.
        for (int i = 0; i < levels.get(0).size(); i++) {
            gtList.get(i).put(QUERY_PAPER_HEADER, "true");
        }

        // Mark the non-query papers
        for (int i = levels.get(0).size(); i < gtList.size(); i++) {
            gtList.get(i).put(QUERY_PAPER_HEADER, "false");
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

    public static List getPaperProjectionForNetwork(GraphTraversalSource traversal, UserQuery query,
                                                    Set<Object> uniqueVertexIds,
                                                    List<List<Vertex>> paperVertices) throws Exception
    {
        // paperVertices is modified on return
        List<Map> gtList = new ArrayList<>();
        String paperIdKey = null;
        String fromHeader = null;
        String toHeader = null;
        int totalGatheredPapers = 0;
        boolean isCitationsGraph = query.RequiresCitationsGraph();
        boolean isReferencesGraph = query.RequiresReferencesGraph();

        if (isCitationsGraph && isReferencesGraph) {
            throw new UnsupportedOperationException("Citations graph and references graph are not supported in the same projection.");
        } else if (!isCitationsGraph && !isReferencesGraph) {
            throw new Exception("A citation or reference paper projection was not specified for requested network.");
        }

        if (query.DataSet().equals("mag")) {
            paperIdKey = "paperId";
        } else if (query.DataSet().equals("wos")) {
            paperIdKey = "wosId";
        } else if (query.DataSet().equals("uspto")) {
            paperIdKey = "patent_id";
        }else {
            throw new Exception("Provided data set must be 'mag' or 'wos' or 'uspto'.");
        }

        if (isCitationsGraph) {
            fromHeader = "From (Citing)";
            toHeader = "To (Cited)";
        } else {
            fromHeader = "From (Referencing)";
            toHeader = "To (Referenced)";
        }

        // Allocate array for cited/referencing papers
        paperVertices.add(new ArrayList<Vertex>());
        List<Vertex> adjacentPapers = paperVertices.get(1);

        for (Vertex qv : paperVertices.get(0)) {
            GraphTraversal gt = traversal.V(qv);

            totalGatheredPapers = paperVertices.get(0).size() + paperVertices.get(1).size();

//            if (query.RequiresCitationsGraph()) {
//                gt = gt.outE("References").inV().dedup();
//            } else if (query.RequiresReferencesGraph()) {
//                gt = gt.inE("References").outV().dedup();
//            }


//            if (query.DataSet().equals("mag")) {
//                if (isCitationsGraph) {
//                    t = traversal.V(qv).outE("References").project("From (Citing)", "To (Cited)").by(__.outV().values("paperId")).by(__.inV().values("paperId"));
//                } else if (isReferencesGraph) {
//                    t = traversal.V(qv).inE("References").project("From (Referencing)", "To (Referenced)").by(__.outV().values("paperId")).by(__.inV().values("paperId"));
//                }
//            } else {
//                if (isCitationsGraph) {
//                    t = traversal.V(qv).outE("References").project("From (Citing)", "To (Cited)").by(__.outV().values("wosId")).by(__.inV().values("wosId"));
//                } else if (isReferencesGraph) {
//                    t = traversal.V(qv).inE("References").project("From (Referencing)", "To (Referenced)").by(__.outV().values("wosId")).by(__.inV().values("wosId"));
//                }
//            }

            if (!query.DataSet().equals("uspto")){
                if (isCitationsGraph) {
                    gt = traversal.V(qv).outE("References").dedup();
                } else {
                    gt = traversal.V(qv).inE("References").dedup();
                }
            }else {
                if (isCitationsGraph) {
                    gt = traversal.V(qv).outE("Cites").dedup();
                } else {
                    gt = traversal.V(qv).inE("Cites").dedup();
                }
            }


            Vertex fromVertex = null;
            Vertex toVertex = null;
            Vertex adjacentVertex = null;
            List<Edge> edgeList = gt.toList();
            boolean isGatheredVertex = false;

            for (Edge e : edgeList) {
                LinkedHashMap<String, Object> csvEntry = new LinkedHashMap<>();

                if (isCitationsGraph) {
                    fromVertex = qv;
                    adjacentVertex = toVertex = e.inVertex();
                } else {
                    adjacentVertex = fromVertex = e.outVertex();
                    toVertex = qv;
                }

                if (totalGatheredPapers >= record_limit) {
                    isGatheredVertex = uniqueVertexIds.contains(adjacentVertex.id());
                } else {
                    if (uniqueVertexIds.add(adjacentVertex.id())) {
                        adjacentPapers.add(adjacentVertex);
                    }

                    isGatheredVertex = true;
                }

                // If the adjacent vertex is in the papers list, then add the
                // edge to the csvEntry map.
                if (isGatheredVertex) {
                    csvEntry.put(fromHeader, fromVertex.value(paperIdKey));
                    csvEntry.put(toHeader, toVertex.value(paperIdKey));
                    gtList.add(csvEntry);
                }
            }
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

    public static List<List<Vertex>> getMAGProjectionForQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        List<List<Vertex>> magVertices = null;

        if (query.HasAbstractSearch())
            throw new UnsupportedOperationException("Search by abstract is not supported");

        if (query.Nodes().stream().anyMatch(n -> n.type.equals(JOURNAL_FIELD))) {
            magVertices = getProjectionForNonPaperQuery(traversal, query, JOURNAL_FIELD);
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(CONFERENCE_INSTANCE_FIELD))) {
            magVertices = getProjectionForNonPaperQuery(traversal, query, CONFERENCE_INSTANCE_FIELD);
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(AUTHOR_FIELD))) {
            magVertices = getProjectionForNonPaperQuery(traversal, query, AUTHOR_FIELD);
        } else {
            magVertices = getProjectionForPaperQueryMAG(traversal, query);
        }

        return magVertices;
    }

    public static List<List<Vertex>> getWOSProjectionForQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (query.HasAbstractSearch())
            throw new UnsupportedOperationException("Search by abstract is not supported");
        return getProjectionForPaperQueryWOS(traversal, query);
    }

    public static List<List<Vertex>> getUSPTOProjectionForQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
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
            return getProjectionForPatentQuery(traversal, query);
        }
    }


    public static List<List<Vertex>> getProjectionForNonPatentQuery(GraphTraversalSource traversal, UserQuery query, String nodeType) throws Exception {
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
        List<List<Vertex>> patents = new ArrayList<>();
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
        patents.add(new ArrayList<Vertex>(patentFilters));
        LOG.info("********* Patents returned **********");
        return patents;
    }


    public static List<List<Vertex>> getProjectionForNonPaperQuery(GraphTraversalSource traversal, UserQuery query, String nodeType) throws Exception {
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
        Set<Vertex> filteredPapers = new HashSet<>();
        List<List<Vertex>> papers = new ArrayList<>();
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
        filteredPapers = intersection(paperFiltersWithConfInst, new ArrayList<>(intersection1));

        LOG.info("size " + filteredPapers.size());

        // Add filtered papers to zeroth level (query papers) of vertices
        papers.add(new ArrayList<Vertex>(filteredPapers));

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

    private static List<List<Vertex>> getProjectionForPaperQueryMAG(GraphTraversalSource traversal, UserQuery query) throws Exception {
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
                        t = t.has(paperNode.type, f.field, Integer.parseInt(f.value));
                    }
                }
            }
        }

        LOG.info("Query: " + t);
        List<List<Vertex>> papers = new ArrayList<>();
        // Allocate list of papers for zeroth level (query papers) of vertices
        papers.add(new ArrayList<Vertex>());
        int batchSize;
        int totalGatheredPapers = 0;

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

            while (gt.hasNext()) {
                totalGatheredPapers = papers.get(0).size();
                if (totalGatheredPapers < record_limit) {
                    batchSize = Math.min(maxBatchSize, record_limit - totalGatheredPapers);
                    papers.get(0).addAll(gt.next(batchSize));
                } else {
                    break;
                }
            }

            if (totalGatheredPapers >= record_limit) {
                break;
            }

        }

        LOG.info("size ****** " + papers.get(0).size());
        return papers;
    }

    private static List<List<Vertex>> getProjectionForPatentQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
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
        List<List<Vertex>> filteredPatents = new ArrayList<>();
        filteredPatents.add(new ArrayList<Vertex>());
        int batchSize;
        int totalGatheredPatents = 0;
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

            while (gt.hasNext()) {
                totalGatheredPatents = filteredPatents.get(0).size();
                if (totalGatheredPatents < record_limit) {
                    batchSize = Math.min(maxBatchSize, record_limit - totalGatheredPatents);
                    filteredPatents.get(0).addAll(gt.next(batchSize));
                } else {
                    break;
                }
            }

            if (totalGatheredPatents >= record_limit) {
                break;
            }
        }
        LOG.info("size ****** " + filteredPatents.get(0).size());
        return filteredPatents;
    }


    /*
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
     */

    private static List<List<Vertex>> getProjectionForPaperQueryWOS(GraphTraversalSource traversal, UserQuery query) throws Exception {
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
        List<List<Vertex>> papers = new ArrayList<>();
        // Allocate list of papers for zeroth level (query papers) of vertices
        papers.add(new ArrayList<Vertex>());
        // Allocate list of papers for first level (cited/referencing papers) of vertices
        int batchSize;
        int totalGatheredPapers = 0;

        while (t.hasNext()) {
            Vertex next = (Vertex) t.next();
            GraphTraversal gt = traversal.V(next);

            for (Node paperNode : query.Nodes()) {
                for (Filter f : paperNode.filters) {
                    if (f.field.equals("year")) {
                        gt = gt.has(paperNode.type, f.field, Integer.valueOf(f.value));
                    } else {
                        gt = gt.has(paperNode.type, f.field, textContains(f.value));
                    }
                }
            }

            while (gt.hasNext()) {
                totalGatheredPapers = papers.get(0).size();
                if (totalGatheredPapers < record_limit) {
                    batchSize = Math.min(maxBatchSize, record_limit - totalGatheredPapers);
                    papers.get(0).addAll(gt.next(batchSize));
                } else {
                    break;
                }
            }

            if (totalGatheredPapers >= record_limit) {
                break;
            }
        }

        return papers;
    }

    public static void removeDuplicateVertices(Set<Object> uniqueIds, List<List<Vertex>> levels) throws Exception {
        for (List<Vertex> verticesList : levels) {
            for (int i = 0; i < verticesList.size(); i++) {
                if (!uniqueIds.add(verticesList.get(i).id())) {
                    verticesList.remove(i);
                    i--;
                }
            }
        }
    }
}
