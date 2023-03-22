package iu.cadre.listeners.job;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.rmi.UnexpectedException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static iu.cadre.listeners.job.util.Constants.*;
import static org.janusgraph.core.attribute.Text.textContains;
import static org.janusgraph.core.attribute.Text.textContainsFuzzy;
import static org.janusgraph.core.attribute.Text.textRegex;

public class UserQuery2Gremlin {
    //public static final String PAPER_FIELD = "Paper";
    public static Integer record_limit;
    private static final Logger LOG = LoggerFactory.getLogger(UserQuery2Gremlin.class);
    private static final String QUERY_PAPER_HEADER = "isQueryPaper";
    private static final int maxBatchSize = 100;

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

        if (source.equals(PATENT_FIELD) && target.equals(PATENT_FIELD))
            return CITATION_FIELD;

        if (source.equals(PATENT_FIELD) && target.equals(GOVERNMENT_ORGANIZATION_FIELD))
            return INTERESTED_IN_FIELD;

        if (source.equals(GOVERNMENT_ORGANIZATION_FIELD) && target.equals(PATENT_FIELD))
            return INTERESTED_IN_FIELD;
 
        if (source.equals(PATENT_FIELD) && target.equals(INVENTOR_FIELD))
            return INVENTOR_OF_FIELD;

        if (source.equals(INVENTOR_FIELD) && target.equals(PATENT_FIELD))
            return INVENTOR_OF_FIELD;

        if (source.equals(INVENTOR_FIELD) && target.equals(LOCATION_FIELD))
            return INVENTOR_LOCATED_IN_FIELD;

        if (source.equals(LOCATION_FIELD) && target.equals(INVENTOR_FIELD))
            return INVENTOR_LOCATED_IN_FIELD;

        if (source.equals(ASSIGNEE_FIELD) && target.equals(LOCATION_FIELD))
            return ASSIGNEE_LOCATED_IN_FIELD;

        if (source.equals(LOCATION_FIELD) && target.equals(ASSIGNEE_FIELD))
            return ASSIGNEE_LOCATED_IN_FIELD;

        if (source.equals(PATENT_FIELD) && target.equals(ASSIGNEE_FIELD))
            return ASSIGNED_TO_FIELD;

        if (source.equals(ASSIGNEE_FIELD) && target.equals(PATENT_FIELD))
            return ASSIGNED_TO_FIELD;
     
        if (source.equals(ATTORNEY_FIELD) && target.equals(PATENT_FIELD))
            return ATTORNEY_OF_FIELD;

        if (source.equals(PATENT_FIELD) && target.equals(ATTORNEY_FIELD))
            return ATTORNEY_OF_FIELD;

        if (source.equals(PATENT_FIELD) && target.equals(USPC_FIELD))
            return USPC_CATEGORY_OF_FIELD;

        if (source.equals(USPC_FIELD) && target.equals(PATENT_FIELD))
            return USPC_CATEGORY_OF_FIELD;

        if (source.equals(PATENT_FIELD) && target.equals(CPC_FIELD))
            return CPC_CATEGORY_OF_FIELD;

        if (source.equals(CPC_FIELD) && target.equals(PATENT_FIELD))
            return CPC_CATEGORY_OF_FIELD;

        if (source.equals(WIPO_FIELD) && target.equals(PATENT_FIELD))
            return WIPO_CATEGORY_OF_FIELD;

        if (source.equals(PATENT_FIELD) && target.equals(WIPO_FIELD))
            return WIPO_CATEGORY_OF_FIELD;

        if (source.equals(IPC_FIELD) && target.equals(PATENT_FIELD))
            return IPC_CATEGORY_OF_FIELD;

        if (source.equals(PATENT_FIELD) && target.equals(IPC_FIELD))
            return IPC_CATEGORY_OF_FIELD;

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
                            if (c.vertexType.equals(PAPER_FIELD)) {
                                t = t.by(__.coalesce(__.values(c.field), __.constant("")));
                            } else if (c.vertexType.equals(AFFILIATION_FIELD)) {
//                              g.V(97581334600).project('Affiliation_displayName').by(both('AuthorOf').hasLabel('Author').bothE().bothV().hasLabel('Affiliation').properties('displayName').value()).fold()
                                //t = t.by(__.both(AUTHOR_OF_FIELD).hasLabel(AUTHOR_FIELD).bothE().bothV().hasLabel(AFFILIATION_FIELD).values(c.field).fold());
                                t = t.by(__.inE(AUTHOR_OF_FIELD).outV().outE(AFFILIATED__WITH_FIELD).inV().values(c.field).fold());
                            } else if (c.vertexType.equals(AUTHOR_FIELD)) {
                                t = t.by(__.inE(edgeLabel(PAPER_FIELD, c.vertexType)).outV().values(c.field).fold());
                            } else {
                                //t = t.by(__.both(edgeLabel(PAPER_FIELD, c.vertexType)).values(c.field).fold());
                                t = t.by(__.outE(edgeLabel(PAPER_FIELD, c.vertexType)).inV().values(c.field).fold());
                            }
                        }else {
                            if (c.vertexType.equals(PATENT_FIELD)) {
                                t = t.by(__.coalesce(__.values(c.field), __.constant("")));
                            } else if (c.vertexType.equals(APPLICATION_CITES_FIELD)) {
                                t = t.by(__.in(CITES_FIELD).values(c.field).fold());
                            } else if (c.vertexType.equals(APPLICATION_BECOMES_FIELD)) {
                                t = t.by(__.in(BECOMES_FIELD).values(c.field).fold());
                            } else if (c.vertexType.equals(INVENTOR_LOCATION_FIELD)) {
                                t = t.by(__.in(INVENTOR_OF_FIELD).out(INVENTOR_LOCATED_IN_FIELD).values(c.field).fold());
                            } else if (c.vertexType.equals(ASSIGNEE_LOCATION_FIELD)) {
                                t = t.by(__.in(ASSIGNED_TO_FIELD).out(ASSIGNEE_LOCATED_IN_FIELD).values(c.field).fold());
                            } else {
                                t = t.by(__.in(edgeLabel(PATENT_FIELD, c.vertexType)).values(c.field).fold());
                            }
                        }
                    }
                    gtList.addAll(t.toList());
                }
            }
        }

        // Mark the query papers.  All query papers
        // are at the front of the list because they come from
        // the first level, so add the mark there.
        for (int i = 0; i < levels.get(0).size(); i++) {
            gtList.get(i).put(QUERY_PAPER_HEADER, "true");
        }

        // Mark the non-query papers
        for (int i = levels.get(0).size(); i < gtList.size(); i++) {
            gtList.get(i).put(QUERY_PAPER_HEADER, "false");
        }

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
            paperIdKey = MAG_PAPER_ID;
        } else if (query.DataSet().equals("wos")) {
            paperIdKey = WOS_PAPER_ID;
        } else if (query.DataSet().equals("uspto")) {
            paperIdKey = USPTO_PATENT_ID;
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

            if (!query.DataSet().equals("uspto")){
                if (isCitationsGraph) {
                    gt = traversal.V(qv).outE("References").dedup();
                } else {
                    gt = traversal.V(qv).inE("References").dedup();
                }
            }else {
                if (isCitationsGraph) {
                    gt = traversal.V(qv).outE("Citation").dedup();
                } else {
                    gt = traversal.V(qv).inE("Citation").dedup();
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
                    // If the paper does already exist in one of the levels,
                    // then declar it gathered so it will appear in
                    // the CSV of citations/references.
                    isGatheredVertex = uniqueVertexIds.contains(adjacentVertex.id());
                } else {
                    // If the paper already exists in the first or second
                    // level, don't add it to adjacent papers
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

        if (!paperNodes.isEmpty() || query.DataSet().equals("mag")) {
            if (query.DataSet().equals("mag")) {
                if (edgeType.equals(AUTHOR_FIELD)) {
                    t = t.outE(AUTHOR_OF_FIELD).inV();
                } else if (edgeType.equals(JOURNAL_FIELD)) {
                    t = t.inE(PUBLISHED_IN_FIELD).outV();
                } else if (edgeType.equals(CONFERENCE_INSTANCE_FIELD)) {
                    t = t.inE(PRESENTED_AT_FIELD).outV();
                } else if (edgeType.equals(AFFILIATION_FIELD)) {
                    t = t.inE(AFFILIATED__WITH_FIELD).outV().outE(AUTHOR_OF_FIELD).inV();
                } else if (edgeType.equals(CONFERENCE_INSTANCE_FIELD)) {
                    t = t.inE(PRESENTED_AT_FIELD).outV();
                } else {
                    t = t.both(edgeLabel(edgeType, PAPER_FIELD));
                }
            } else {
                t = t.both(edgeLabel(edgeType, PAPER_FIELD));
            }
        } else {
            t = t.both(edgeLabel(edgeType, PAPER_FIELD));
        }

        if (!paperNodes.isEmpty()) {
            for (Node paperNode : paperNodes) {
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, true, t);
/*
                for (Filter f : paperNode.filters) {
                    if (f.field.equals("year")) {
                        t = t.has(paperNode.type, f.field, Integer.parseInt(f.value));
                    } else if (f.field.equals("doi")) {
                        t = t.has(paperNode.type, f.field, f.value);
                    } else {
                        t = t.has(paperNode.type, f.field, support_fuzzy_queries ? textContainsFuzzy(f.value) : textContains(f.value));
                    }
                }
*/
            }
        }

        return t;
    }

    private static GraphTraversal getPatentsByNodeTypeAndFilter(GraphTraversal t, UserQuery query, String vertexType) throws Exception {
        List<Node> patentNodes = query.Nodes().stream().filter(n -> n.type.equals(PATENT_FIELD)).collect(Collectors.toList());

        if (vertexType.equals(APPLICATION_CITES_FIELD)) {
            t = t.out(CITES_FIELD);
        } else if (vertexType.equals(APPLICATION_BECOMES_FIELD)) {
            t = t.out(BECOMES_FIELD);
        } else if (vertexType.equals(INVENTOR_LOCATION_FIELD)) {
            t = t.in(INVENTOR_LOCATED_IN_FIELD).out(INVENTOR_OF_FIELD);
        } else if (vertexType.equals(ASSIGNEE_LOCATION_FIELD)) {
            t = t.in(ASSIGNEE_LOCATED_IN_FIELD).out(ASSIGNED_TO_FIELD);
        } else {
            //Edges from all other vertex types to patent vertices are outgoing edges from
            //the non-patent vertex to the patent vertex
            //t = t.both(edgeLabel(vertexType, PATENT_FIELD));
            t = t.out(edgeLabel(vertexType, PATENT_FIELD));
        }

        // Apply any patent filters
        if (!patentNodes.isEmpty()) {
            for (Node patentNode : patentNodes) {
                t = applyFilters(query.DataSet(), patentNode.type, patentNode.filters, true, t);
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
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(AFFILIATION_FIELD))) {
            magVertices = getProjectionForNonPaperQuery(traversal, query, AFFILIATION_FIELD);
        }else if (query.Nodes().stream().anyMatch(n -> n.type.equals(FIELD_OF_STUDY_FIELD))) {
            magVertices = getProjectionForNonPaperQuery(traversal, query, FIELD_OF_STUDY_FIELD);
        }else {
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
        List <List<Vertex>> projection = null;

        if (query.Nodes().stream().anyMatch(n -> n.type.equals(INVENTOR_FIELD)) ||
            query.Nodes().stream().anyMatch(n -> n.type.equals(ASSIGNEE_FIELD)) ||
            query.Nodes().stream().anyMatch(n -> n.type.equals(ATTORNEY_FIELD)) ||
            query.Nodes().stream().anyMatch(n -> n.type.equals(APPLICATION_CITES_FIELD)) ||
            query.Nodes().stream().anyMatch(n -> n.type.equals(APPLICATION_BECOMES_FIELD)) ||
            query.Nodes().stream().anyMatch(n -> n.type.equals(INVENTOR_LOCATION_FIELD)) ||
            query.Nodes().stream().anyMatch(n -> n.type.equals(ASSIGNEE_LOCATION_FIELD)) ||
            query.Nodes().stream().anyMatch(n -> n.type.equals(GOVERNMENT_ORGANIZATION_FIELD)) ||
            query.Nodes().stream().anyMatch(n -> n.type.equals(USPC_FIELD)) ||
            query.Nodes().stream().anyMatch(n -> n.type.equals(CPC_FIELD)) ||
            query.Nodes().stream().anyMatch(n -> n.type.equals(WIPO_FIELD)) ||
            query.Nodes().stream().anyMatch(n -> n.type.equals(IPC_FIELD))) {
            projection = getProjectionForNonPatentQuery(query, traversal);
        } else if (query.Nodes().stream().anyMatch(n -> n.type.equals(PATENT_FIELD))) {
            projection = getProjectionForPatentQuery(query, traversal);
        }

        return projection;
    }


    public static List<List<Vertex>> getProjectionForNonPatentQuery(UserQuery query, GraphTraversalSource traversal) throws Exception {
        // Apply non-patent node filters
        Set<Vertex> applicationCitesVertices = applyFiltersByNodeType(query, traversal, APPLICATION_CITES_FIELD, APPLICATION_CITES_FIELD, false);
        Set<Vertex> applicationBecomesVertices = applyFiltersByNodeType(query, traversal, APPLICATION_BECOMES_FIELD, APPLICATION_BECOMES_FIELD, false);
        Set<Vertex> govOrganizationVertices = applyFiltersByNodeType(query, traversal, GOVERNMENT_ORGANIZATION_FIELD, GOVERNMENT_ORGANIZATION_FIELD, false);
        Set<Vertex> inventorVertices = applyFiltersByNodeType(query, traversal, INVENTOR_FIELD, INVENTOR_FIELD, false);
        Set<Vertex> inventorLocationVertices = applyFiltersByNodeType(query, traversal, INVENTOR_LOCATION_FIELD, LOCATION_FIELD, false);
        Set<Vertex> assigneeLocationVertices = applyFiltersByNodeType(query, traversal, ASSIGNEE_LOCATION_FIELD, LOCATION_FIELD, false);
        Set<Vertex> assigneeVertices = applyFiltersByNodeType(query, traversal, ASSIGNEE_FIELD, ASSIGNEE_FIELD, false);
        Set<Vertex> attorneyVertices = applyFiltersByNodeType(query, traversal, ATTORNEY_FIELD, ATTORNEY_FIELD, false);
        Set<Vertex> uspcVertices = applyFiltersByNodeType(query, traversal, USPC_FIELD, USPC_FIELD, false);
        Set<Vertex> cpcVertices = applyFiltersByNodeType(query, traversal, CPC_FIELD, CPC_FIELD, false);
        Set<Vertex> wipoVertices = applyFiltersByNodeType(query, traversal, WIPO_FIELD, WIPO_FIELD, false);
        Set<Vertex> ipcVertices = applyFiltersByNodeType(query, traversal, IPC_FIELD, IPC_FIELD, false);

        LOG.info("********* Non-patent vertices returned *******************");
        LOG.info("********* size application cites vertices      : " + applicationCitesVertices.size());
        LOG.info("********* size application becomes vertices    : " + applicationBecomesVertices.size());
        LOG.info("********* size government organization vertices: " + govOrganizationVertices.size());
        LOG.info("********* size inventor vertices               : " + inventorVertices.size());
        LOG.info("********* size inventor location vertices      : " + inventorLocationVertices.size());
        LOG.info("********* size assignee location vertices      : " + assigneeLocationVertices.size());
        LOG.info("********* size assignee vertices               : " + assigneeVertices.size());
        LOG.info("********* size attorney vertices               : " + attorneyVertices.size());
        LOG.info("********* size uspc vertices                   : " + uspcVertices.size());
        LOG.info("********* size cpc vertices                    : " + cpcVertices.size());
        LOG.info("********* size wipo vertices                   : " + wipoVertices.size());
        LOG.info("********* size ipc vertices                    : " + ipcVertices.size());

        Set<Vertex> patentsWithApplicationCites = getPatentsByNodeTypeAndFilter(query, traversal, APPLICATION_CITES_FIELD, applicationCitesVertices);
        Set<Vertex> patentsWithApplicationBecomes = getPatentsByNodeTypeAndFilter(query, traversal, APPLICATION_BECOMES_FIELD, applicationBecomesVertices);
        Set<Vertex> patentsWithGovOrganization = getPatentsByNodeTypeAndFilter(query, traversal, GOVERNMENT_ORGANIZATION_FIELD, govOrganizationVertices);
        Set<Vertex> patentsWithInventor = getPatentsByNodeTypeAndFilter(query, traversal, INVENTOR_FIELD, inventorVertices);
        Set<Vertex> patentsWithInventorLocation = getPatentsByNodeTypeAndFilter(query, traversal, INVENTOR_LOCATION_FIELD, inventorLocationVertices);
        Set<Vertex> patentsWithAssigneeLocation = getPatentsByNodeTypeAndFilter(query, traversal, ASSIGNEE_LOCATION_FIELD, assigneeLocationVertices);
        Set<Vertex> patentsWithAssignee = getPatentsByNodeTypeAndFilter(query, traversal, ASSIGNEE_FIELD, assigneeVertices);
        Set<Vertex> patentsWithAttorney = getPatentsByNodeTypeAndFilter(query, traversal, ATTORNEY_FIELD, attorneyVertices);
        Set<Vertex> patentsWithUspc = getPatentsByNodeTypeAndFilter(query, traversal, USPC_FIELD, uspcVertices);
        Set<Vertex> patentsWithCpc = getPatentsByNodeTypeAndFilter(query, traversal, CPC_FIELD, cpcVertices);
        Set<Vertex> patentsWithWipo = getPatentsByNodeTypeAndFilter(query, traversal, WIPO_FIELD, wipoVertices);
        Set<Vertex> patentsWithIpc = getPatentsByNodeTypeAndFilter(query, traversal, IPC_FIELD, ipcVertices);

        List<List<Vertex>> patents = new ArrayList<>();
        LOG.info("********* Patent nodes returned ***********");
        LOG.info("********* size patentsWithApplicationCites   : " + patentsWithApplicationCites.size());
        LOG.info("********* size patentsWithApplicationBecomes : " + patentsWithApplicationBecomes.size());
        LOG.info("********* size patentsWithGovOrganization    : " + patentsWithGovOrganization.size());
        LOG.info("********* size patentsWithInventor           : " + patentsWithInventor.size());
        LOG.info("********* size patentsWithInventorLocation   : " + patentsWithInventorLocation.size());
        LOG.info("********* size patentsWithAssigneeLocation   : " + patentsWithAssigneeLocation.size());
        LOG.info("********* size patentsWithAssignee           : " + patentsWithAssignee.size());
        LOG.info("********* size patentsWithAttorney           : " + patentsWithAttorney.size());
        LOG.info("********* size patentsWithUspc               : " + patentsWithUspc.size());
        LOG.info("********* size patentsWithCpc                : " + patentsWithCpc.size());
        LOG.info("********* size patentsWithWipo               : " + patentsWithWipo.size());
        LOG.info("********* size patentsWithIpc                : " + patentsWithIpc.size());

        Set<Vertex> patentFilters = new HashSet<>();

        // The sets can be large, set them to null
        // to facilitate garbage collection when no longer needed.
        Set<Vertex> union1 = union(patentsWithApplicationCites, patentsWithApplicationBecomes);
        patentsWithApplicationCites = null; patentsWithApplicationBecomes = null;
        Set<Vertex> intersection1 = intersection(patentsWithGovOrganization, union1);
        patentsWithGovOrganization = null; union1 = null;
        Set<Vertex> intersection2 = intersection(patentsWithInventor, intersection1);
        patentsWithInventor = null; intersection1 = null;
        Set<Vertex> intersection3 = intersection(patentsWithInventorLocation, intersection2);
        patentsWithInventorLocation = null; intersection2 = null;
        Set<Vertex> intersection4 = intersection(patentsWithAssigneeLocation, intersection3);
        patentsWithAssigneeLocation = null; intersection3 = null;
        Set<Vertex> intersection5 = intersection(patentsWithAssignee, intersection4);
        patentsWithAssignee = null; intersection4 = null;
        Set<Vertex> intersection6 = intersection(patentsWithAttorney, intersection5);
        patentsWithAttorney = null; intersection5 = null;
        Set<Vertex> intersection7 = intersection(patentsWithUspc, intersection6);
        patentsWithUspc = null; intersection6 = null;
        Set<Vertex> intersection8 = intersection(patentsWithCpc, intersection7);
        patentsWithCpc = null; intersection7 = null;
        Set<Vertex> intersection9 = intersection(patentsWithWipo, intersection8);
        patentsWithWipo = null; intersection8 = null;
        patentFilters = intersection(patentsWithIpc, intersection9);
        patentsWithIpc = null; intersection9 = null;

        patents.add(new ArrayList<Vertex>(patentFilters));
        LOG.info("********* Intersected patent nodes: " + patentFilters.size());
        LOG.info("********* Completed non-patents query **********");
        return patents;
    }


    public static List<List<Vertex>> getProjectionForNonPaperQuery(GraphTraversalSource traversal, UserQuery query, String nodeType) throws Exception {
        // Apply other filters
        List<Node> authorNodes = query.Nodes().stream().filter(n -> n.type.equals(AUTHOR_FIELD)).collect(Collectors.toList());
        List<Node> journalNodes = query.Nodes().stream().filter(n -> n.type.equals(JOURNAL_FIELD)).collect(Collectors.toList());
        List<Node> confInstanceNodes = query.Nodes().stream().filter(n -> n.type.equals(CONFERENCE_INSTANCE_FIELD)).collect(Collectors.toList());
        List<Node> affiliationNodes = query.Nodes().stream().filter(n -> n.type.equals(AFFILIATION_FIELD)).collect(Collectors.toList());
        List<Node> fieldOfStudyNodes = query.Nodes().stream().filter(n -> n.type.equals(FIELD_OF_STUDY_FIELD)).collect(Collectors.toList());

        GraphTraversal t1 = traversal.V();
        GraphTraversal t2 = traversal.V();
        GraphTraversal t3 = traversal.V();
        GraphTraversal t4 = traversal.V();
        GraphTraversal t5 = traversal.V();
        List<Vertex> nonPaperNodesList1 = new ArrayList<>();
        List<Vertex> nonPaperNodesList2 = new ArrayList<>();
        List<Vertex> nonPaperNodesList3 = new ArrayList<>();
        List<Vertex> nonPaperNodesList4 = new ArrayList<>();
        List<Vertex> nonPaperNodesList5 = new ArrayList<>();
        if (!authorNodes.isEmpty()){
            for (Node n : authorNodes) {
                t1 = applyFilters(query.DataSet(), n.type, n.filters, false, t1);
                nonPaperNodesList1 = t1.toList();
/*
                for (Filter f : n.filters) {
                    t1 = t1.has(n.type, f.field, textContains(f.value));
                    if (nodeType.equals(AUTHOR_FIELD)){
                        nonPaperNodesList1 = t1.limit(record_limit*2).toList();
                    }else {
                        nonPaperNodesList1 = t1.toList();
                    }
                }
*/
            }
        }
        if (!journalNodes.isEmpty()){
            for (Node n : journalNodes) {
                t2 = applyFilters(query.DataSet(), n.type, n.filters, false, t2);
                nonPaperNodesList2 = t2.toList();
/*
                for (Filter f : n.filters) {
                    t2 = t2.has(n.type, f.field, textContains(f.value));
                    nonPaperNodesList2 = t2.limit(record_limit*2).toList();
                }
*/
            }
        }
        if (!confInstanceNodes.isEmpty()){
            for (Node n : confInstanceNodes) {
                t3 = applyFilters(query.DataSet(), n.type, n.filters, false, t3);
                nonPaperNodesList3 = t3.toList();
/*
                for (Filter f : n.filters) {
                    t3 = t3.has(n.type, f.field, textContains(f.value));
                    nonPaperNodesList3 = t3.limit(record_limit*2).toList();
                }
*/
            }
        }
        if (!affiliationNodes.isEmpty()){
            for (Node n : affiliationNodes) {
                t4 = applyFilters(query.DataSet(), n.type, n.filters, false, t4);
                nonPaperNodesList4 = t4.toList();
            }
        }
        if (!fieldOfStudyNodes.isEmpty()){
            for (Node n : fieldOfStudyNodes) {
                t5 = applyFilters(query.DataSet(), n.type, n.filters, false, t5);
                nonPaperNodesList5 = t5.toList();
            }
        }

        LOG.info("********* Non paper nodes returned ***********");
        LOG.info("********* size authornodes ***********" + nonPaperNodesList1.size());
        LOG.info("********* size journalnodes *********** " + nonPaperNodesList2.size());
        LOG.info("********* size confInstanceNodes *********** " + nonPaperNodesList3.size());
        LOG.info("********* size affiliationNodes *********** " + nonPaperNodesList4.size());
        LOG.info("********* size fieldOfStudyNodes *********** " + nonPaperNodesList5.size());

        List<Vertex> paperFiltersWithAuthor = new ArrayList<>();
        List<Vertex> paperFiltersWithJournal = new ArrayList<>();
        List<Vertex> paperFiltersWithConfInst = new ArrayList<>();
        List<Vertex> paperFiltersWithAffiliation = new ArrayList<>();
        List<Vertex> paperFiltersWithFieldOfStudy = new ArrayList<>();
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

        for (Vertex nonPaperVertex : nonPaperNodesList4){
            GraphTraversal gt  = getPaperFilter(traversal.V(nonPaperVertex), query, AFFILIATION_FIELD);
            while (gt.hasNext()) {
                paperFiltersWithAffiliation.addAll(gt.next(batchSize));
            }
        }

        for (Vertex nonPaperVertex : nonPaperNodesList5){
            GraphTraversal gt  = getPaperFilter(traversal.V(nonPaperVertex), query, FIELD_OF_STUDY_FIELD);
            while (gt.hasNext()) {
                paperFiltersWithFieldOfStudy.addAll(gt.next(batchSize));
            }
        }

        LOG.info("Size authorPaperFilters " + paperFiltersWithAuthor.size());
        LOG.info("Size journalPaperFilters " + paperFiltersWithJournal.size());
        LOG.info("Size confPaperFilters " + paperFiltersWithConfInst.size());
        LOG.info("Size affiliationFilters " + paperFiltersWithAffiliation.size());
        LOG.info("Size fieldOfStudyFilters " + paperFiltersWithFieldOfStudy.size());

        // If all the sets of nodes are not empty or some of them are empty because
        // no filters were specified, then proceed to intersect the sets to form the
        // final list of papers.  If some of the sets are empty, but filters were
        // specified on them, then the intersection will be the empty set, so do
        // nothing.
        if ((!paperFiltersWithAuthor.isEmpty() || authorNodes.isEmpty()) &&
            (!paperFiltersWithJournal.isEmpty() || journalNodes.isEmpty()) &&
            (!paperFiltersWithConfInst.isEmpty() || confInstanceNodes.isEmpty()) &&
            (!paperFiltersWithAffiliation.isEmpty() || affiliationNodes.isEmpty()) &&
            (!paperFiltersWithFieldOfStudy.isEmpty() || fieldOfStudyNodes.isEmpty()))
        {
           Set<Vertex> intersection1 = intersection(paperFiltersWithAuthor, paperFiltersWithJournal);
           Set<Vertex> intersection2 = intersection(paperFiltersWithConfInst, new ArrayList<>(intersection1));
           Set<Vertex> intersection3 = intersection(paperFiltersWithAffiliation, new ArrayList<>(intersection2));
           filteredPapers = intersection(paperFiltersWithFieldOfStudy, new ArrayList<>(intersection3));
        }

        LOG.info("size " + filteredPapers.size());

        // Add filtered papers to zeroth level (query papers) of vertices
        papers.add(new ArrayList<Vertex>(filteredPapers));

        LOG.info("********* Papers returned **********");
        return papers;
    }


    private static <T> Set<T> intersection(List<T> list1, List<T> list2) {
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

    private static <T> Set<T> intersection(List<T> list1, Set<T> list2) {
        Set<T> list = new HashSet<>();
        if (list1.isEmpty()){
            return new HashSet<>(list2);
        }else if (list2.isEmpty()){
            return list2;
        }

        Set<T> first = new HashSet<>(list1);

        for (T t : list2) {
            if(first.contains(t)) {
                list.add(t);
            }
        }
        return list;
    }

    private static <T> Set<T> intersection(Set<T> set1, Set<T> cumulativeIntersection) {
        Set<T> intersectionSet = cumulativeIntersection;

        if (set1 != null) {
           if (cumulativeIntersection == null) {
              // The cumulative intersection has yet to be initialized
              intersectionSet = set1;
           } else {
              // If either set is empty, the intersection is empty
              if (set1.isEmpty()) {
                 intersectionSet = set1;
              } else if (cumulativeIntersection.isEmpty()) {
                 intersectionSet = cumulativeIntersection;
              } else {
                 intersectionSet = set1;
                 intersectionSet.retainAll(cumulativeIntersection);
              }
           }
        }

        return intersectionSet;
    }

    private static <T> Set<T> union(Set<T> set1, Set<T> set2) {
        Set<T> unionSet = null;

        // WARNING: The return reference can be aliased to an
        // input reference.  This is fine for our purposes and
        // it improves code performance.

        if (set1 != null) {
           if (set2 == null) {
              unionSet = set1;
           } else {
              if (set1.isEmpty()) {
                 unionSet = set2;
              } else if (set2.isEmpty()) {
                 unionSet = set1;
              } else {
                 unionSet = set1;
                 unionSet.addAll(set2);
              }
           }
        }

        return unionSet;
    }

    private static List<List<Vertex>> getProjectionForPaperQueryMAG(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (query.Nodes().stream().anyMatch(n -> !n.type.equals(PAPER_FIELD)))
            throw new UnexpectedException("Can't filter non-paper nodes");

        GraphTraversal t = traversal.V();

        for (Node paperNode : query.Nodes()) {
//          Get all the papers with one filters first
            if (paperNode.filters.stream().anyMatch(f -> f.field.equals("doi"))){
                //System.out.println("Calling single-filter applyFilter on doi");
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "doi", t);
/*
                for (Filter f : paperNode.filters) {
                    if (f.field.equals("doi")) {
                        t = t.has(paperNode.type, f.field, f.value);
                    }
                }
*/
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("paperTitle"))){
                //System.out.println("Calling single-filter applyFilter on paperTitle");
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "paperTitle", t);
/*
                for (Filter f : paperNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("paperTitle")) {
                        t = t.has(paperNode.type, f.field, textContains(f.value));
                    }
                }
*/
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("date"))){
                //System.out.println("Calling single-filter applyFilter on year");
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "date", t);

/*
                for (Filter f : paperNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("year")) {
                        t = t.has(paperNode.type, f.field, Integer.parseInt(f.value));
                    }
                }
*/
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("year"))){
                //System.out.println("Calling single-filter applyFilter on year");
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "year", t);

/*
                for (Filter f : paperNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("year")) {
                        t = t.has(paperNode.type, f.field, Integer.parseInt(f.value));
                    }
                }
*/
            }
        }

        LOG.info("Query: " + t);
        List<List<Vertex>> papers = new ArrayList<>();
        // Allocate list of papers for zeroth level (query papers) of vertices
        papers.add(new ArrayList<Vertex>());
        int batchSize;
        int totalGatheredPapers = 0;

        //System.out.println("Applying all filters");

        while (t.hasNext()) {
            Vertex next = (Vertex) t.next();
            GraphTraversal gt = traversal.V(next);
            List<Node> paperNodes = query.Nodes().stream().filter(n -> n.type.equals(PAPER_FIELD)).collect(Collectors.toList());

            //System.out.println("Applying all paper node filters to next vertex");
            for (Node paperNode : paperNodes) {
                gt = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, true, gt);
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

    private static List<List<Vertex>> getProjectionForPatentQuery(UserQuery query, GraphTraversalSource traversal) throws Exception {
        if (query.Nodes().stream().anyMatch(n -> !n.type.equals(PATENT_FIELD)))
            throw new UnexpectedException("Can't filter non-patent nodes");
        GraphTraversal t = traversal.V();
        for (Node patentNode : query.Nodes()) {
            // Get all the papers with one filters first, this can reduce search time
            // when additional filters are applied.  Order matters here; we try to apply the
            // filter in a way that will limit time spent on expensive searches.
            if (patentNode.filters.stream().anyMatch(f -> f.field.equals("patent_id"))) {
                t = applyFilters(query.DataSet(), patentNode.type, patentNode.filters, "patent_id", t);
            } else if (patentNode.filters.stream().anyMatch(f -> f.field.equals("patent_title"))) {
                t = applyFilters(query.DataSet(), patentNode.type, patentNode.filters, "patent_title", t);
            } else if (patentNode.filters.stream().anyMatch(f -> f.field.equals("patent_abstract"))) {
                t = applyFilters(query.DataSet(), patentNode.type, patentNode.filters, "patent_abstract", t);
            } else if (patentNode.filters.stream().anyMatch(f -> f.field.equals("patent_date"))) {
                t = applyFilters(query.DataSet(), patentNode.type, patentNode.filters, "patent_date", t);
            } else if (patentNode.filters.stream().anyMatch(f -> f.field.equals("patent_year"))) {
                t = applyFilters(query.DataSet(), patentNode.type, patentNode.filters, "patent_year", t);
            } else if (patentNode.filters.stream().anyMatch(f -> f.field.equals("patent_type"))) {
                t = applyFilters(query.DataSet(), patentNode.type, patentNode.filters, "patent_type", t);
            }
        }

        LOG.info("Patent initial query: " + t);
        List<List<Vertex>> filteredPatents = new ArrayList<>();
        filteredPatents.add(new ArrayList<Vertex>());
        int batchSize;
        int totalGatheredPatents = 0;

        while (t.hasNext()) {
            Vertex next = (Vertex) t.next();
            GraphTraversal gt = traversal.V(next);
            List<Node> patentNodes = query.Nodes().stream().filter(n -> n.type.equals(PATENT_FIELD)).collect(Collectors.toList());

            for (Node patentNode : patentNodes) {
                gt = applyFilters(query.DataSet(), patentNode.type, patentNode.filters, true, gt);
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
            if (paperNode.filters.stream().anyMatch(f -> f.field.equals("journaliso"))){
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "journaliso", t);
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("issn"))){
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "issn", t);
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("papertitle"))){
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "papertitle", t);
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("journaltitle"))){
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "journaltitle", t);
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("lc_standard_names"))){
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "lc_standard_names", t);
/*
                for (Filter f : paperNode.filters) {
                    LOG.info(f.field);
                    if (f.field.equals("lc_standard_names")) {
                        t = t.has(paperNode.type, f.field, textContains(f.value));
                    }
                }
*/
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("publicationyear"))){
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "publicationyear", t);
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("conferencetitle"))){
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "conferencetitle", t);
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("doi"))){
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "doi", t);
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("full_address"))){
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "full_address", t);
            }else if (paperNode.filters.stream().anyMatch(f -> f.field.equals("isopenaccess"))){
                t = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, "isopenaccess", t);
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

            //gt = gt.has("Paper", "publicationYear", 1985);

            for (Node paperNode : query.Nodes()) {
                //System.out.println("Calling applyFilter on node " + paperNode.type);
                gt = applyFilters(query.DataSet(), paperNode.type, paperNode.filters, true, gt);
/*
                for (Filter f : paperNode.filters) {
                    if (f.field.equals("DOI")) {
                        gt = gt.has(paperNode.type, f.field, f.value);
                    } else if (f.field.equals("publicationYear")) {
                        //System.out.println("In publicationYear: " + f.field + " = " + f.value);
                        gt = gt.has(paperNode.type, f.field, Integer.valueOf(f.value));
                    } else if (f.field.equals("articleTitle")) {
                        //System.out.println("In articleTitle: " + f.field + " = " + f.value);
                        gt = gt.has(paperNode.type, f.field, textContains(f.value));

                        if (f.operator.contentEquals("OR")) {
                           gt = gt.or();
                        } else if (f.operator.contentEquals("AND")) {
                           gt = gt.and();
                        }
                    } else {
                        gt = gt.has(paperNode.type, f.field, textContains(f.value));
                    }
                }
*/
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

    // This function is used to apply a single filter for each node block
    // of the given node type contained in the JSON query.  This is used to
    // perform a quick reduction in the total number of vertexes that must 
    // must be searched in the database.
    private static GraphTraversal applyFilters(String dataset, String nodeType, ArrayList<Filter> filters, String targetField, GraphTraversal t) throws Exception {
       // Loop over all filters in the JSON node section
       for (int i = 0; i < filters.size(); i++) {
          // Find the start of a block of filters for the given field
          if (filters.get(i).field.contentEquals(targetField)) {
             ArrayList<Filter> filterBlock = new ArrayList<Filter>();
             int j = i+1;

             filterBlock.add(filters.get(i));

/*
             System.out.println("---------");
             System.out.println("New block");
             System.out.println("---------");
             System.out.println("filterBlock(0).field   : " + filterBlock.get(0).field);
             System.out.println("filterBlock(0).operator: " + filterBlock.get(0).operator);
             System.out.println("filterBlock(0).value   : " + filterBlock.get(0).value);
*/

             // Determine the extent of the and/or block;
             while (j < filters.size() && !filters.get(j-1).operator.contentEquals("")) {
                // Get the target filters that are in the block
                if (filters.get(j).field.contentEquals(targetField)) {
                   filterBlock.add(filters.get(j));
                   j++;

/*
                   System.out.println("---------");
                   System.out.println("filterBlock(" + (filterBlock.size()-1) + ").field   : " + filterBlock.get(filterBlock.size()-1).field);
                   System.out.println("filterBlock(" + (filterBlock.size()-1) + ").operator: " + filterBlock.get(filterBlock.size()-1).operator);
                   System.out.println("filterBlock(" + (filterBlock.size()-1) + ").value   : " + filterBlock.get(filterBlock.size()-1).value);
*/
 
                   if (filters.get(j-1).operator.contentEquals("")) {
                      break;
                   }
                } else {
                   break;
                }
             }

             i = j-1;
             t = generateTraversalFromFilters(dataset, nodeType, filterBlock, t);
          }
       }

       return t;
    }

    private static GraphTraversal applyFilters(String dataset, String nodeType, ArrayList<Filter> filters, boolean isDocumentQuery, GraphTraversal t) throws Exception {
       // Loop over all filters in the JSON node section
       for (int i = 0; i < filters.size(); i++) {
          // Find the start of a block of filters
          ArrayList<Filter> filterBlock = new ArrayList<Filter>();
          int j = i+1;

          //System.out.println("Adding filter " + i);
          filterBlock.add(filters.get(i));


/*
          System.out.println("New block");
          System.out.println("---------");
          System.out.println("filterBlock(0).field   : " + filterBlock.get(0).field);
          System.out.println("filterBlock(0).operator: " + filterBlock.get(0).operator);
          System.out.println("filterBlock(0).value   : " + filterBlock.get(0).value);
*/

          // Determine the extent of the and/or block;
/*
          System.out.println("filters.size(): " + filters.size());
          System.out.println("j             : " + j);
          System.out.println("Before while loop");
          System.out.println("filters.get(" + (j-1) + ").operator: " + filters.get(j-1).operator);
*/
          while (j < filters.size() && !filters.get(j-1).operator.contentEquals("")) {
/*
             System.out.println("Adding filter " + j);
*/
             filterBlock.add(filters.get(j));
             j++;


/*
             System.out.println("---------");
             System.out.println("filterBlock(" + (filterBlock.size()-1) + ").field   : " + filterBlock.get(filterBlock.size()-1).field);
             System.out.println("filterBlock(" + (filterBlock.size()-1) + ").operator: " + filterBlock.get(filterBlock.size()-1).operator);
             System.out.println("filterBlock(" + (filterBlock.size()-1) + ").value   : " + filterBlock.get(filterBlock.size()-1).value);
*/

 
             if (filters.get(j-1).operator.contentEquals("")) {
                break;
             }
          }

          i = j-1;
          t = generateTraversalFromFilters(dataset, nodeType, filterBlock, t);

          if (!isDocumentQuery) {
             t = t.limit(record_limit*2);
          }
       }

       return t;
    }


    private static GraphTraversal generateTraversalFromFilters(String dataset, String nodeType, ArrayList<Filter> filterBlock, GraphTraversal t) throws Exception {
       ArrayList<Object> values = new ArrayList<>();

       // Please read the search predicate documentation at:
       // https://old-docs.janusgraph.org/0.3.2/search-predicates.html
       // before modifying how certain fields are searched on.
       if (dataset.contentEquals("wos")) {
          for (Filter f : filterBlock) {
             if (f.field.contentEquals("doi")) {
                values.add(f.value);
             } else if (f.field.contentEquals("journaliso")) {
                values.add(f.value);
             } else if (f.field.contentEquals("issn")) {
                values.add(f.value);
             } else if (f.field.contentEquals("isopenaccess")) {
                values.add(f.value);
             } else if (f.field.contentEquals("publicationyear")) {
                values.add(Integer.valueOf(f.value));
             } else if (f.field.contentEquals("lc_standard_names")) {
                String regexStr = ".*" + f.value.toLowerCase() + ".*";
                values.add(textRegex(regexStr));
             } else {
                values.add(textContains(f.value));
             }
          }
       } else if (dataset.contentEquals("mag")) {
          for (Filter f : filterBlock) {
             if (f.field.contentEquals("doi")) {
                values.add(f.value);
             } else if (f.field.contentEquals("date")) {
                values.add(applyDateFilter(DATE_FORMAT, MAG_TIME_ZONE, f.value));
             } else if (f.field.contentEquals("year")) {
                values.add(Integer.valueOf(f.value));
             } else if (f.field.contentEquals("affiliationId")) {
                values.add(Long.valueOf(f.value));
             } else if (f.field.contentEquals("authorId")) {
                values.add(Long.valueOf(f.value));
             } else if (f.field.contentEquals("journalId")) {
                values.add(Long.valueOf(f.value));
             } else if (f.field.contentEquals("issn")) {
                values.add(f.value);
             } else if (f.field.contentEquals("fieldOfStudyId")) {
                values.add(Long.valueOf(f.value));
             } else if (f.field.contentEquals("conferenceInstanceId")) {
                values.add(Long.valueOf(f.value));
             } else {
                values.add(textContains(f.value));
             }
          }
       } else if (dataset.contentEquals("uspto")) {
          for (Filter f : filterBlock) {
             if (f.field.contentEquals("patent_id") ||
                 f.field.contentEquals("patent_type") ||
                 f.field.contentEquals("application_id") ||
                 f.field.contentEquals("application_type") ||
                 f.field.contentEquals("organization_id") ||
                 f.field.contentEquals("fedagency_name") ||
                 f.field.contentEquals("inventor_id") ||
                 f.field.contentEquals("location_id") ||
                 f.field.contentEquals("city") ||
                 f.field.contentEquals("state") ||
                 f.field.contentEquals("country") ||
                 f.field.contentEquals("state_fips") ||
                 f.field.contentEquals("county_fips") ||
                 f.field.contentEquals("assignee_id") ||
                 f.field.contentEquals("assignee_organization") ||
                 f.field.contentEquals("attorney_id") ||
                 f.field.contentEquals("uspc_id") ||
                 f.field.contentEquals("cpc_id") ||
                 f.field.contentEquals("ipc_id") ||
                 f.field.contentEquals("level") ||
                 f.field.contentEquals("category_label")) {
                values.add(f.value);
             } else if (f.field.contentEquals("inventor_name") ||
                        f.field.contentEquals("assignee_name") ||
                        f.field.contentEquals("attorney_name")) {
                values.add(f.value);
             } else if (f.field.contentEquals("patent_date") ||
                        f.field.contentEquals("filing_date")) {
                values.add(applyDateFilter(DATE_FORMAT, USPTO_TIME_ZONE, f.value));
             } else if (f.field.contentEquals("wipo_field_id")) {
                values.add(Float.valueOf(f.value));
             } else if (f.field.contentEquals("patent_year") ||
                        f.field.contentEquals("assignee_type")) {
                values.add(Integer.valueOf(f.value));
             } else if (f.field.contentEquals("patent_title")) {
                values.add(textContains(f.value));
             } else if (f.field.contentEquals("patent_abstract")) {
                values.add(textContains(f.value));
             } else {
                values.add(textContains(f.value));
             }
          }
       } else {
          throw new Exception("Only WoS, MAG, and USPTO datasets are supported by traversal generation");
       }

       if (filterBlock.size() == 1) {
          t = t.has(nodeType, filterBlock.get(0).field, values.get(0));
       } else {
          // Generate call to OR or AND method
          if (filterBlock.get(0).operator.contentEquals("OR")) {
             if (filterBlock.size() == 2) {
                Filter f0 = filterBlock.get(0);
                Filter f1 = filterBlock.get(1);

                t = t.or(__.has(nodeType, f0.field, values.get(0)),
                         __.has(nodeType, f1.field, values.get(1)));
             } else if (filterBlock.size() == 3) {
                Filter f0 = filterBlock.get(0);
                Filter f1 = filterBlock.get(1);
                Filter f2 = filterBlock.get(2);

                t = t.or(__.has(nodeType, f0.field, values.get(0)),
                         __.has(nodeType, f1.field, values.get(1)),
                         __.has(nodeType, f2.field, values.get(2)));
             } else if (filterBlock.size() == 4) {
                Filter f0 = filterBlock.get(0);
                Filter f1 = filterBlock.get(1);
                Filter f2 = filterBlock.get(2);
                Filter f3 = filterBlock.get(3);

                t = t.or(__.has(nodeType, f0.field, values.get(0)),
                         __.has(nodeType, f1.field, values.get(1)),
                         __.has(nodeType, f2.field, values.get(2)),
                         __.has(nodeType, f3.field, values.get(3)));
             } else if (filterBlock.size() == 5) {
                Filter f0 = filterBlock.get(0);
                Filter f1 = filterBlock.get(1);
                Filter f2 = filterBlock.get(2);
                Filter f3 = filterBlock.get(3);
                Filter f4 = filterBlock.get(4);

                t = t.or(__.has(nodeType, f0.field, values.get(0)),
                         __.has(nodeType, f1.field, values.get(1)),
                         __.has(nodeType, f2.field, values.get(2)),
                         __.has(nodeType, f3.field, values.get(3)),
                         __.has(nodeType, f4.field, values.get(4)));
             } else if (filterBlock.size() == 6) {
                Filter f0 = filterBlock.get(0);
                Filter f1 = filterBlock.get(1);
                Filter f2 = filterBlock.get(2);
                Filter f3 = filterBlock.get(3);
                Filter f4 = filterBlock.get(4);
                Filter f5 = filterBlock.get(5);

                t = t.or(__.has(nodeType, f0.field, values.get(0)),
                         __.has(nodeType, f1.field, values.get(1)),
                         __.has(nodeType, f2.field, values.get(2)),
                         __.has(nodeType, f3.field, values.get(3)),
                         __.has(nodeType, f4.field, values.get(4)),
                         __.has(nodeType, f5.field, values.get(5)));
             } else if (filterBlock.size() == 7) {
                Filter f0 = filterBlock.get(0);
                Filter f1 = filterBlock.get(1);
                Filter f2 = filterBlock.get(2);
                Filter f3 = filterBlock.get(3);
                Filter f4 = filterBlock.get(4);
                Filter f5 = filterBlock.get(5);
                Filter f6 = filterBlock.get(6);

                t = t.or(__.has(nodeType, f0.field, values.get(0)),
                         __.has(nodeType, f1.field, values.get(1)),
                         __.has(nodeType, f2.field, values.get(2)),
                         __.has(nodeType, f3.field, values.get(3)),
                         __.has(nodeType, f4.field, values.get(4)),
                         __.has(nodeType, f5.field, values.get(5)),
                         __.has(nodeType, f6.field, values.get(6)));
             } else if (filterBlock.size() == 8) {
                Filter f0 = filterBlock.get(0);
                Filter f1 = filterBlock.get(1);
                Filter f2 = filterBlock.get(2);
                Filter f3 = filterBlock.get(3);
                Filter f4 = filterBlock.get(4);
                Filter f5 = filterBlock.get(5);
                Filter f6 = filterBlock.get(6);
                Filter f7 = filterBlock.get(7);

                t = t.or(__.has(nodeType, f0.field, values.get(0)),
                         __.has(nodeType, f1.field, values.get(1)),
                         __.has(nodeType, f2.field, values.get(2)),
                         __.has(nodeType, f3.field, values.get(3)),
                         __.has(nodeType, f4.field, values.get(4)),
                         __.has(nodeType, f5.field, values.get(5)),
                         __.has(nodeType, f6.field, values.get(6)),
                         __.has(nodeType, f7.field, values.get(7)));
             } else if (filterBlock.size() == 9) {
                Filter f0 = filterBlock.get(0);
                Filter f1 = filterBlock.get(1);
                Filter f2 = filterBlock.get(2);
                Filter f3 = filterBlock.get(3);
                Filter f4 = filterBlock.get(4);
                Filter f5 = filterBlock.get(5);
                Filter f6 = filterBlock.get(6);
                Filter f7 = filterBlock.get(7);
                Filter f8 = filterBlock.get(8);

                t = t.or(__.has(nodeType, f0.field, values.get(0)),
                         __.has(nodeType, f1.field, values.get(1)),
                         __.has(nodeType, f2.field, values.get(2)),
                         __.has(nodeType, f3.field, values.get(3)),
                         __.has(nodeType, f4.field, values.get(4)),
                         __.has(nodeType, f5.field, values.get(5)),
                         __.has(nodeType, f6.field, values.get(6)),
                         __.has(nodeType, f7.field, values.get(7)),
                         __.has(nodeType, f8.field, values.get(8)));
             } else if (filterBlock.size() == 10) {
                Filter f0 = filterBlock.get(0);
                Filter f1 = filterBlock.get(1);
                Filter f2 = filterBlock.get(2);
                Filter f3 = filterBlock.get(3);
                Filter f4 = filterBlock.get(4);
                Filter f5 = filterBlock.get(5);
                Filter f6 = filterBlock.get(6);
                Filter f7 = filterBlock.get(7);
                Filter f8 = filterBlock.get(8);
                Filter f9 = filterBlock.get(9);

                t = t.or(__.has(nodeType, f0.field, values.get(0)),
                         __.has(nodeType, f1.field, values.get(1)),
                         __.has(nodeType, f2.field, values.get(2)),
                         __.has(nodeType, f3.field, values.get(3)),
                         __.has(nodeType, f4.field, values.get(4)),
                         __.has(nodeType, f5.field, values.get(5)),
                         __.has(nodeType, f6.field, values.get(6)),
                         __.has(nodeType, f7.field, values.get(7)),
                         __.has(nodeType, f8.field, values.get(8)),
                         __.has(nodeType, f9.field, values.get(9)));
             }
          } else {
             throw new Exception("and() operation not supported in traversals"); 
          }
       }

       return t;
    }
          

    private static Set<Vertex> applyFiltersByNodeType(UserQuery query, GraphTraversalSource traversal, String nodeLabel, String vertexLabel, boolean isDocumentQuery) throws Exception {
       List<Node> nodeList = query.Nodes().stream().filter(n -> n.type.equals(nodeLabel)).collect(Collectors.toList());
       GraphTraversal t = traversal.V();
       Set<Vertex> vertexSet = null;

       if (!nodeList.isEmpty()) {
          vertexSet = new HashSet<>();

          for (Node n : nodeList) {
             t = applyFilters(query.DataSet(), vertexLabel, n.filters, isDocumentQuery, t);
             vertexSet.addAll(t.toList());
          }
       }

       return vertexSet;
    }

    private static Set<Vertex> getPatentsByNodeTypeAndFilter(UserQuery query, GraphTraversalSource traversal, String nodeLabel, Set<Vertex> nonPatentNodesSet) throws Exception {
       Set<Vertex> filteredPatents = null;

       if (nonPatentNodesSet == null) {
          filteredPatents = new HashSet<Vertex>();

          for (Vertex nonPatentVertex : nonPatentNodesSet) {
             GraphTraversal gt = getPatentsByNodeTypeAndFilter(traversal.V(nonPatentVertex), query, nodeLabel);
             while (gt.hasNext()) {
                filteredPatents.addAll(gt.next(maxBatchSize));
             }
          }
       }

       return filteredPatents;
    }

    private static Object applyDateFilter(String dateFormat, String timeZone, String dateRangeStr) throws Exception {
        Object filterValue = null;
        int slashIndex = dateRangeStr.indexOf('/');

        if (slashIndex == -1) {
            throw new Exception("Invalid date range format.  Missing '/'.");
        }

        DateFormat dateFormatter = new SimpleDateFormat(dateFormat);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(timeZone));
        //dateFormatter.setLenient(true);

        if (slashIndex == 0) {
            String startDateStr = dateRangeStr.substring(1);
            Date date = dateFormatter.parse(startDateStr);

            if (date == null) {
                throw new Exception("Parse of date filter failed");
            }

            filterValue = date;
        } else {
            String startDateStr = dateRangeStr.substring(0, slashIndex);
            String endDateStr = dateRangeStr.substring(slashIndex+1);
            Date startDate = dateFormatter.parse(startDateStr);
            Date endDate = dateFormatter.parse(endDateStr);

            if (startDate == null || endDate == null) {
                throw new Exception("Parse of startDate or endDate failed");
            }

            filterValue = P.between(startDate, endDate);
        }

        return filterValue;
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
