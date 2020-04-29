package iu.cadre.listeners.job;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import iu.cadre.listeners.job.util.ConfigReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.janusgraph.core.attribute.Text.textContainsFuzzy;

public class JobListener {
    private static final String QUEUE_NAME = "testQueue";
    private static final JsonParser jsonParser = new JsonParser();
    protected static final Log LOG = LogFactory.getLog(JobListener.class);

    public static void main(String[] args) {

    }

    public static String getFileName(String jobID, String jobName){
        String fileName = "";
        if (jobID.equals(jobName) || jobName.isEmpty()){
           fileName = jobID;
        }else {
            fileName = jobName + '_' + jobID;
        }
        return fileName;
    }

    public static GraphTraversalSource getJanusTraversal() throws Exception{
        try {
            String janusConfig = ConfigReader.getJanusPropertiesFile();
            final JanusGraph graph = JanusGraphFactory.open(janusConfig);
            StandardJanusGraph standardGraph = (StandardJanusGraph) graph;
            // get graph management
            JanusGraphManagement mgmt = standardGraph.openManagement();
            // you code using 'mgmt' to perform any management related operations
            // using graph to do traversal
            JanusGraphTransaction graphTransaction = graph.newTransaction();
            return graphTransaction.traversal();
        }catch (Exception e){
            LOG.error( "Unable to create graph traversal object. Error : " +e.getMessage());
            throw new Exception("Unable to create graph traversal object.", e);
        }
    }

    public static void poll_queue() throws Exception {
        SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_2).build();
        List<String> outputFiltersSingle = new ArrayList<String>();

        String jobUpdateStatement = "UPDATE user_job SET job_status = 'RUNNING', modified_on = CURRENT_TIMESTAMP WHERE job_id = ?";
        String fileInsertStatement = "INSERT INTO query_result(job_id,efs_path, file_checksum, data_type, authenticity, created_by, created_on) " +
                "VALUES(?,?,?,?,?,?,current_timestamp)";


        while (true) {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(QUEUE_NAME)
                    .build();

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

//            	'Body': '{"job_name": "Test", "filters": [{"field": "year", "value": "2005", "operation": "AND"}, {"field": "journal_display_name", "value": "science", "operation": ""}], "output": [{"field": "paper_id", "type": "single"}, {"field": "year", "type": "single"}, {"field": "original_title", "type": "single"}, {"field": "authors_display_name", "type": "single"}, {"field": "journal_display_name", "type": "single"}, {"field": "citations", "type": "network", "degree": 1}], "dataset": "mag", "job_id": "5d3f63cf-3f49-4a24-9b0f-3461a0485078", "username": "mjzwk4tsmv2hi", "user_id": 25}'
            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
            // print out the messages
            for (Message m : messages) {
                String messageBody = m.body();
                JsonElement messageBodyJElement = jsonParser.parse(messageBody);
                JsonObject messageBodyJObj = messageBodyJElement.getAsJsonObject();
                String dataset = messageBodyJObj.get("dataset").getAsString();
                String jobId = messageBodyJObj.get("job_id").getAsString();
                String jobName = messageBodyJObj.get("job_name").getAsString();
                String userName = messageBodyJObj.get("username").getAsString();
                String userId = messageBodyJObj.get("user_id").getAsString();
                JsonObject graphJson = messageBodyJObj.get("graph").getAsJsonObject();
                JsonArray outputFields = messageBodyJObj.get("csv_output").getAsJsonArray();

                String metaDBUrl = "jdbc:postgresql://" + ConfigReader.getMetaDBHost() + ":" + ConfigReader.getMetaDBPort() + "/" + ConfigReader.getMetaDBName();
                try (Connection conn = DriverManager.getConnection(
                        metaDBUrl, ConfigReader.getMetaDBUsername(), ConfigReader.getMetaDBPWD());
                     PreparedStatement preparedStatement = conn.prepareStatement(jobUpdateStatement)) {

                    preparedStatement.setString(1, jobId);
                    preparedStatement.executeUpdate();

                    String efsRootDir = ConfigReader.getEFSRootListenerDir();
                    String efsSubPath = ConfigReader.getEFSSubPathListenerDir();
                    String efsPath = efsRootDir + efsSubPath;
                    String graphImportDir = ConfigReader.getEFSGraphImportDir();
                    String userQueryResultDir = efsPath + '/' + userName + "/query-results";
                    File directory = new File(userQueryResultDir);
                    if (! directory.exists()) {
                        directory.mkdirs();
                    }
                    String fileName = getFileName(jobId, jobName);
                    String csvPath = userQueryResultDir + File.separator + fileName + ".csv";
                    String graphMLFile = userQueryResultDir + File.separator + fileName + ".xml";

                    if (dataset.equals("mag")){
                        GraphTraversalSource janusTraversal = getJanusTraversal();
                        GraphTraversalSource subgraph = getSubGraphForQuery(janusTraversal, graphJson, outputFiltersSingle);
                        subgraph.io(graphMLFile).write().iterate();
                        //  to convert to csv, use BenF method
                    }
                } catch (SQLException e) {
                    LOG.error("Error while updating meta db. Error is : " + e.getMessage());
                    throw new Exception("Error while updating meta db",e);
                }
            }
        }
    }

    // we will support paper to paper citations for now
    // filters": [{"field": "year", "field": "2005", "operation": "AND"}, {"field": "journal_display_name", "value": "science", "operation": ""}]
    // sg = g.V().and(has('Paper', 'paperTitle', 'ladle pouring guide'), has('Paper', 'year', '1950')).outE('References').subgraph('sg1').cap('sg1').next()
    //  TinkerGraph tg = (TinkerGraph)traversal.V().and(has(vertexLabel, fieldName, teX VCFFF VGFNBVC xtContainsFuzzy(fieldValue)), has(vertexLabel, "year", 1990)).inE("AuthorOf").subgraph("org_auth2").cap("org_auth2").next();
    //  GraphTraversalSource sg = tg.traversal();
    //  sg.io("/home/ubuntu/unicorn_chathuri_2.xml").write().iterate();
//    public static GraphTraversalSource getSubGraphForQueryForDegree1(GraphTraversalSource traversal, JsonArray filterFields, List<String> outputFields){
//        int size = filterFields.size();
//        Map<GraphTraversal<Object, Object>, String> hasFilterWithOperatorMap = new LinkedHashMap<GraphTraversal<Object, Object>, String>();
//        GraphTraversal<Vertex, Vertex> filterTraversal = null;
//        if (size > 0) {
//            JsonObject firstJobject = filterFields.get(0).getAsJsonObject();
//            JsonObject lastJObject = filterFields.get(size - 1).getAsJsonObject();
//            if (firstJobject == lastJObject) {
//                String field = firstJobject.get("field").getAsString();
//                String value = firstJobject.get("value").getAsString();
//                String operation = firstJobject.get("operation").getAsString();
//                if (field.equals("year")) {
//                    filterTraversal = traversal.V().has("Paper", "year", Integer.valueOf(value));
//                } else if (field.equals("journal_display_name")) {
//                    filterTraversal = traversal.V().has("Journal", "journalName", textContainsFuzzy(value));
//                } else if (field.equals("authors_display_name")) {
//                    filterTraversal = traversal.V().has("Author", "authorsDisplayName", textContainsFuzzy(value));
//                } else if (field.equals("doi")) {
//                    filterTraversal = traversal.V().has("Paper", "doi", textContainsFuzzy(value));
//                } else if (field.equals("conference_display_name")) {
//                    filterTraversal = traversal.V().has("Paper", "conferenceDisplayName", textContainsFuzzy(value));
//                } else if (field.equals("paper_title")) {
//                    filterTraversal = traversal.V().has("Paper", "paperTitle", textContainsFuzzy(value));
//                } else if (field.equals("paper_abstract")) {
//                    filterTraversal = traversal.V().has("Paper", "paperAbstract", textContainsFuzzy(value));
//                }
//            }else {
//                String firstField = firstJobject.get("field").getAsString();
//                String firstValue = firstJobject.get("value").getAsString();
//                String firstOperation = firstJobject.get("operation").getAsString();
//                if (firstField.equals("year")) {
//                    GraphTraversal<Object, Object> yearHas = has("Paper", "year", Integer.valueOf(firstValue));
//                    filterTraversal = addFilters(traversal.V(), firstOperation, yearHas);
//                } else if (firstField.equals("journal_display_name")) {
//                    GraphTraversal<Object, Object> journalNameHas = has("Paper", "journalName", textContainsFuzzy(firstValue));
//                    filterTraversal = addFilters(traversal.V(), firstOperation, journalNameHas);
//                } else if (firstField.equals("authors_display_name")) {
//                    GraphTraversal<Object, Object> authorDisplayNameHas = has("Paper", "authorsDisplayName", textContainsFuzzy(firstValue));
//                    filterTraversal = addFilters(traversal.V(), firstOperation, authorDisplayNameHas);
//                } else if (firstField.equals("doi")) {
//                    GraphTraversal<Object, Object> doiHas = has("Paper", "doi", textContainsFuzzy(firstValue));
//                    filterTraversal = addFilters(traversal.V(), firstOperation, doiHas);
//                } else if (firstField.equals("conference_display_name")) {
//                    GraphTraversal<Object, Object> conferenceDNHas = has("Paper", "conferenceDisplayName", textContainsFuzzy(firstValue));
//                    filterTraversal = addFilters(traversal.V(), firstOperation, conferenceDNHas);
//                } else if (firstField.equals("paper_title")) {
//                    GraphTraversal<Object, Object> paperTitleHas = has("Paper", "paperTitle", textContainsFuzzy(firstValue));
//                    filterTraversal = addFilters(traversal.V(), firstOperation, paperTitleHas);
//                } else if (firstField.equals("paper_abstract")) {
//                    GraphTraversal<Object, Object> paperAbstractHas = has("Paper", "paperAbstract", textContainsFuzzy(firstValue));
//                    filterTraversal = addFilters(traversal.V(), firstOperation, paperAbstractHas);
//                }
//                for (int i = 1; i < size -1; i++) {
//                    JsonObject filterObject = filterFields.get(i).getAsJsonObject();
//                    String field = filterObject.get("field").getAsString();
//                    String value = filterObject.get("value").getAsString();
//                    String operation = filterObject.get("operation").getAsString();
//                    if (field.equals("year")) {
//                        GraphTraversal<Object, Object> yearHas = has("Paper", "year", Integer.valueOf(value));
//                        filterTraversal = addFilters(filterTraversal, firstOperation, yearHas);
//                    } else if (field.equals("journal_display_name")) {
//                        GraphTraversal<Object, Object> journalNameHas = has("Paper", "journalName", textContainsFuzzy(value));
//                        filterTraversal = addFilters(filterTraversal, firstOperation, journalNameHas);
//                    } else if (field.equals("authors_display_name")) {
//                        GraphTraversal<Object, Object> authorDisplayNameHas = has("Paper", "authorsDisplayName", textContainsFuzzy(value));
//                        filterTraversal = addFilters(filterTraversal, firstOperation, authorDisplayNameHas);
//                    } else if (field.equals("doi")) {
//                        GraphTraversal<Object, Object> doiHas = has("Paper", "doi", textContainsFuzzy(value));
//                        filterTraversal = addFilters(filterTraversal, firstOperation, doiHas);
//                    } else if (field.equals("conference_display_name")) {
//                        GraphTraversal<Object, Object> conferenceDNHas = has("Paper", "conferenceDisplayName", textContainsFuzzy(value));
//                        filterTraversal = addFilters(filterTraversal, firstOperation, conferenceDNHas);
//                    } else if (field.equals("paper_title")) {
//                        GraphTraversal<Object, Object> paperTitleHas = has("Paper", "paperTitle", textContainsFuzzy(value));
//                        filterTraversal = addFilters(filterTraversal, firstOperation, paperTitleHas);
//                    } else if (field.equals("paper_abstract")) {
//                        GraphTraversal<Object, Object> paperAbstractHas = has("Paper", "paperAbstract", textContainsFuzzy(value));
//                        filterTraversal = addFilters(filterTraversal, firstOperation, paperAbstractHas);
//                    }
//                }
//                String lastField = lastJObject.get("field").getAsString();
//                String lastValue = lastJObject.get("value").getAsString();
//                String lastOperation = lastJObject.get("operation").getAsString();
//                if (lastField.equals("year")) {
//                    filterTraversal = filterTraversal.has("Paper", "year", Integer.valueOf(lastValue));
//                } else if (lastField.equals("journal_display_name")) {
//                    filterTraversal = filterTraversal.has("Paper", "journalName", textContainsFuzzy(lastValue));
//                } else if (lastField.equals("authors_display_name")) {
//                    filterTraversal = filterTraversal.has("Paper", "authorsDisplayName", textContainsFuzzy(lastValue));
//                } else if (lastField.equals("doi")) {
//                    filterTraversal = filterTraversal.has("Paper", "doi", textContainsFuzzy(lastValue));
//                } else if (lastField.equals("conference_display_name")) {
//                    filterTraversal = filterTraversal.has("Paper", "conferenceDisplayName", textContainsFuzzy(lastValue));
//                } else if (lastField.equals("paper_title")) {
//                    filterTraversal = filterTraversal.has("Paper", "paperTitle", textContainsFuzzy(lastValue));
//                } else if (lastField.equals("paper_abstract")) {
//                    filterTraversal = filterTraversal.has("Paper", "paperAbstract", textContainsFuzzy(lastValue));
//                }
//            }
//
//        }
//        TinkerGraph tg = (TinkerGraph)filterTraversal.outE("References").subgraph("sg1").cap("sg1").next();
//        GraphTraversalSource sg = tg.traversal();
//        return sg;
//    }

//    public static GraphTraversal<Vertex, Vertex> addFilters(GraphTraversal<Vertex, Vertex> inputTraversal, String operator, GraphTraversal<Object, Object> hasFilter){
//        if (operator.equals("and")){
//            return inputTraversal.and(hasFilter);
//        }else if (operator.equals("or")){
//            return inputTraversal.or(hasFilter);
//        }else {
//            return null;
//        }
//    }
//
//    public static List<String> getTraversalPath(JsonArray graphFields){
//        List<String> vertices = new ArrayList<String>();
//        for (int i=0; i < graphFields.size(); i++) {
//            JsonObject vertexObject = graphFields.get(i).getAsJsonObject();
//            String vertexType = vertexObject.get("vertexType").getAsString();
//            vertices.add(vertexType);
//        }
//        return vertices;
//    }

    // inE : paper - journal : publishedIn
    // inE : author - paper : authorof
    // inE : paper -> conferenceInstance : presentedAt
//    public static List<String> getEdgeTypes (List<String> vertices){
//        List<String> edgesList = new ArrayList<String>();
//        for(int i=0; i < vertices.size() -1; i++){
//            String startVertex = vertices.get(i);
//            String nextVertex = vertices.get(i+1);
//            if (startVertex.equals("paper") && nextVertex.equals("journal")){
//                edgesList.add("outE:PublishedInFixed");
//            }else if (startVertex.equals("paper") && nextVertex.equals("conferenceInstance")){
//                edgesList.add("inE:PresentedAt");
//            }else if (startVertex.equals("journal") && nextVertex.equals("paper")){
//                edgesList.add("inE:PublishedInFixed");
//            }else if (startVertex.equals("author") && nextVertex.equals("paper")){
//                edgesList.add("inE:AuthorOf");
//            }else if (startVertex.equals("conferenceInstance") && nextVertex.equals("paper")){
//                edgesList.add("outE:PresentedAt");
//            }else if (startVertex.equals("paper") && nextVertex.equals("author")){
//                edgesList.add("outE:AuthorOf");
//            }
//        }
//        return edgesList;
//    }

//    public static GraphTraversalSource getSubGraphForQueryForDegree1(GraphTraversalSource traversal, JsonArray graphFields, List<String> outputFields){
//        GraphTraversal<Vertex, Vertex> filterTraversal = traversal.V();
//        List<String> traversalPath = getTraversalPath(graphFields);
//        List<String> edgeTypesList = getEdgeTypes(traversalPath);
//        boolean isNetworkEnabled = false;
//        if (graphFields.size() > 1){
//            JsonObject lastVertex = graphFields.get(graphFields.size() - 1).getAsJsonObject();
//            String lastvertexType = lastVertex.get("vertexType").getAsString();
//            if (lastvertexType.equals("Paper")){
//                isNetworkEnabled = true;
//            }
//        }
//        int vertexCount = 0;
//        List<GraphTraversal<Vertex, Vertex>> filtersPerVertexList = new ArrayList<>();
//        for (String vertexType : traversalPath){
//            for (int i=0; i < graphFields.size(); i++){
//                JsonObject vertexObject = graphFields.get(i).getAsJsonObject();
//                String vertexTypeList = vertexObject.get("vertexType").getAsString();
//                if (vertexTypeList.equals(vertexType)){
//                    JsonArray filters = vertexObject.get("filters").getAsJsonArray();
//                    GraphTraversal<Vertex, Vertex> vertexVertexGraphTraversal = addFiltersForVertex(filterTraversal, filters, vertexTypeList);
//                    filtersPerVertexList.add(vertexVertexGraphTraversal);
//                }
//            }
//        }
//
//        GraphTraversal<Vertex, Vertex> seedVertexGraphTraversal = filtersPerVertexList.get(0);
//        for (int j=0; j< filtersPerVertexList.size()-1; j++){
//            GraphTraversal<Vertex, Vertex> vertexGraphTraversal = filtersPerVertexList.get(j);
//            String edgeType = edgeTypesList.get(j);
//            String[] splits = edgeType.split(":");
//            String direction = splits[0];
//            String edgeName = splits[1];
//            if (direction.equals("inE")){
//                vertexGraphTraversal = seedVertexGraphTraversal.inE(edgeName).subgraph("sg1").outV();
//            }else {
//                vertexGraphTraversal = seedVertexGraphTraversal.outE(edgeName).subgraph("sg1").outV();
//            }
//        }
//
//    }

//    public static Map<String, List<String[]>> getHasFilters(JsonArray nodes){
//        Map<String, List<String[]>> hasFilterMap = new LinkedHashMap<>();
//        for (int i=0; i < nodes.size(); i++){
//            JsonObject vertexObject = nodes.get(i).getAsJsonObject();
//            String vertexType = vertexObject.get("vertexType").getAsString();
//            JsonArray filters = vertexObject.get("filters").getAsJsonArray();
//            List<String[]> hasFilters = new ArrayList<>();
//            for (int j = 0; j < filters.size(); j++) {
//                JsonObject filterField = filters.get(j).getAsJsonObject();
//                String field = filterField.get("field").getAsString();
//                String value = filterField.get("value").getAsString();
//                String[] fieldValues = new String[]{field, value};
//                String operator = filterField.get("operator").getAsString();
//                hasFilters.add(fieldValues);
//            }
//            hasFilterMap.put(vertexType, hasFilters);
//
//        }
//        return hasFilterMap;
//    }

    public static Map<String, List<Object>> getASLabelFilters(JsonArray nodes){
        Map<String, List<Object>> hasFilterMap = new LinkedHashMap<>();
        for (int i=0; i < nodes.size(); i++){
            String label = "label" + (i+1);
            JsonObject vertexObject = nodes.get(i).getAsJsonObject();
            String vertexType = vertexObject.get("vertexType").getAsString();
            JsonArray filters = vertexObject.get("filters").getAsJsonArray();
            List< Object> hasFilters = new ArrayList<>();
            for (int j = 0; j < filters.size(); j++) {
                JsonObject filterField = filters.get(j).getAsJsonObject();
                String field = filterField.get("field").getAsString();
                String value = filterField.get("value").getAsString();
                String[] fieldValues = new String[]{field, value};
                String operator = filterField.get("operator").getAsString();
                if (!field.equals("year") && !field.equals("doi")){
                    GraphTraversal<Object, Object> asLabelWithFilters = __.as(label).has(vertexType, field, textContainsFuzzy(value));
                    hasFilters.add(asLabelWithFilters);
                }else {
                    GraphTraversal<Object, Object> asLabelWithFilters = __.as(label).has(vertexType, field, Integer.valueOf(value));
                    hasFilters.add(asLabelWithFilters);
                }
            }
            hasFilterMap.put(vertexType, hasFilters);
        }
        return hasFilterMap;
    }

    public static GraphTraversalSource getSubGraphForQuery(GraphTraversalSource traversal, JsonObject graphFields, List<String> outputFields){
        GraphTraversal<Vertex, Vertex> filterTraversal = traversal.V();
        JsonArray nodes = graphFields.get("nodes").getAsJsonArray();
        JsonArray edges = graphFields.get("edges").getAsJsonArray();
        Map<String, List<Object>> asLabelFilters = getASLabelFilters(nodes);
        int count = 1;
        List<Object> allMatchClauses = new ArrayList<>();
        for (String vertexType : asLabelFilters.keySet()){
            String label1 = "label" + count;
            count++;
            String label2 = "label" + count;
            List<Object> hasFilterListPerVertex = asLabelFilters.get(vertexType);
            allMatchClauses.addAll(hasFilterListPerVertex);

            for (int i=0; i < edges.size(); i++){
                JsonObject edgeJson = edges.get(i).getAsJsonObject();
                String sourceVertex = edgeJson.get("source").getAsString();
                String targetVertex = edgeJson.get("target").getAsString();
                String relation = edgeJson.get("relation").getAsString();
                if (sourceVertex.equals("paper") && targetVertex.equals("journal")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(relation).subgraph("sg1").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (sourceVertex.equals("paper") && targetVertex.equals("conferenceInstance")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(relation).subgraph("sg1").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (sourceVertex.equals("journal") && targetVertex.equals("paper")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(relation).subgraph("sg1").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (sourceVertex.equals("author") && targetVertex.equals("paper")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(relation).subgraph("sg1").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (sourceVertex.equals("conferenceInstance") && targetVertex.equals("paper")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(relation).subgraph("sg1").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (sourceVertex.equals("paper") && targetVertex.equals("author")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(relation).subgraph("sg1").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }
            }
        }
        TinkerGraph tg = (TinkerGraph)filterTraversal.match((Traversal<?, ?>) allMatchClauses).cap("sg").next();
        GraphTraversalSource sg = tg.traversal();
        return sg;
    }


//    public static GraphTraversal<Vertex, Vertex> addFiltersForVertex(GraphTraversal<Vertex, Vertex> inputTraversal, JsonArray filtersForVertex, String vertexType){
//        if (filtersForVertex.size() > 1 ){
//            for (int j = 0; j < filtersForVertex.size(); j++){
//                JsonObject filterField = filtersForVertex.get(j).getAsJsonObject();
//                String field = filterField.get("field").getAsString();
//                String value = filterField.get("value").getAsString();
//                String operator = filterField.get("operator").getAsString();
//                GraphTraversal<Object, Object> hasFilter = null;
//                if (!field.equals("year") && !field.equals("doi")){
//                    hasFilter = has(vertexType, field, textContainsFuzzy(value));
//                }else {
//                    hasFilter = has(vertexType, field, Integer.valueOf(value));
//                }
//                if (operator.equals("and")){
//                    inputTraversal = inputTraversal.and(hasFilter);
//                }else if (operator.equals("or")){
//                    inputTraversal = inputTraversal.or(hasFilter);
//                }
//            }
//        }else {
//            JsonObject filterField = filtersForVertex.get(0).getAsJsonObject();
//            String field = filterField.get("field").getAsString();
//            String value = filterField.get("value").getAsString();
//            if (!field.equals("year") && !field.equals("doi")){
//                inputTraversal = inputTraversal.has(vertexType, field, textContainsFuzzy(value));
//            }else {
//                inputTraversal = inputTraversal.has(vertexType, field, Integer.valueOf(value));
//            }
//        }
//        return inputTraversal;
//    }
}
