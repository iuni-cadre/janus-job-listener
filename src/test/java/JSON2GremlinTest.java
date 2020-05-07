import com.google.common.collect.Iterators;
import com.google.gson.JsonParser;
import iu.cadre.listeners.job.GremlinGraphWriter;
import iu.cadre.listeners.job.JSON2Gremlin;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import com.google.gson.JsonObject;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;
import static org.junit.jupiter.api.Assertions.*;

import org.json.JSONObject;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class JSON2GremlinTest {
    //public static final String JOURNAL_FIELD = "JournalRev";
    public static final String JOURNAL_FIELD = "JournalFixed";
    static GraphTraversalSource g;

    @BeforeAll
    static void build_graph() {
        JanusGraph graph = JanusGraphFactory.build().set(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory")
                .set(TIMESTAMP_PROVIDER.toStringWithoutRoot(), "NANO").open();
        g = graph.traversal();

        g.addV("Paper").property("paperTitle", "full case study report upplandsbondens sweden").as("v1").
                addV("Author").property("name", "joe").as("v2").
                addE("AuthorOf").property("test_edge_property", "test_edge_property_value").from("v2").to("v1").iterate();

        g.addV(JOURNAL_FIELD).property("normalizedName", "the open acoustics journal").as("v1").
                addV("Paper").property("name", "coolpaper").as("v2").
                addE("PublishedIn").from("v2").to("v1").iterate();

        for (int i = 0; i < 20; ++i)
            g.addV("Paper").property("paperTitle", String.format("Paper%s", i)).
                    property("year", "1900").as("v1").
                    addV("Author").property("name", "joe").as("v2").
                    addE("AuthorOf").from("v2").to("v1").iterate();

        for (int i = 0; i < 20; ++i)
            g.addV("Paper").property("paperTitle", String.format("Paper%s", i + 1945)).
                    property("year", 1945f).as("v1").
                    addV("Author").property("name", "jane").as("v2").
                    addE("AuthorOf").from("v2").to("v1").iterate();

        Vertex journal = g.V().hasLabel(JOURNAL_FIELD).next();
        for (int i = 0; i < 5; ++i)
            g.addV("Paper").property("paperTitle", String.format("Paper%s", i + 2001)).
                    property("year", 2001).as("v1").
                    addE("PublishedInRev").from("v1").to(journal).iterate();
        g.tx().commit();
    }

    private JsonObject create_json(final String source, final String target) {
        String s = "{\n" +
                   "    \"job_name\": \"Job Name\",\n" +
                   "    \"dataset\": \"mag\",\n" +
                   "    \"job_id\": \"sassas1221\",\n" +
                   "    \"username\": \"abc\",\n" +
                   "    \"user_id\": \"1\",\n" +
                   "    \"graph\": {\n" +
                   "        \"nodes\": [\n" +
                   "            {\n" +
                   "                \"vertexType\": \"Paper\",\n" +
                   "                \"filters\": [\n" +
                   "                    {\n" +
                   "                        \"field\": \"paperTitle\",\n" +
                   "                        \"filterType\": \"is\",\n" +
                   "                        \"value\": \"paper2001\",\n" +
                   "                        \"operator\": \"\"\n" +
                   "                    }\n" +
                   "        ]\n" +
                   "      }], \"edges\" :  [\n" +
                   "      {\n" +
                   "                \"source\": \"" + source + "\",\n" +
                   "                \"target\": \"" + target + "\",\n" +
                   "                \"relation\": \"PublishedInRev\"\n" +
                   "            }\n" +
                   "        ]\n" +
                   "    }\n" +
                   "}";
        JsonParser jsonParser = new JsonParser();
        JsonObject messageBodyJElement = jsonParser.parse(s).getAsJsonObject();
        return messageBodyJElement.get("graph").getAsJsonObject();
    }

    @Test
    void load_graph__throws_if_no_dataset_specified() {
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("job_name", "Job Name");
        JSONObject obj = new JSONObject(query);
        assertThrows(JSONException.class, () -> {
            JSON2Gremlin.load_graph(g, obj);
        });
    }

    @Test
    void load_graph__throws_if_not_mag_dataset() {
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("job_name", "Job Name");
        query.put("dataset", "unknown dataset");
        JSONObject obj = new JSONObject(query);
        assertThrows(UnsupportedOperationException.class, () -> {
            JSON2Gremlin.load_graph(g, obj);
        });
    }

    @Test
    void load_graph__returns_all_papers_without_filters() {
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("job_name", "Job Name");
        query.put("dataset", "mag");

        try {
            query.put("graph", new JSONArray("[{vertexType: \"Paper\"}]"));
            JSONObject obj = new JSONObject(query);
            TinkerGraph tg = JSON2Gremlin.load_graph(g, obj);
            assertEquals(89, Iterators.size(tg.vertices()));
            assertEquals(47, Iterators.size(tg.edges()));
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Test
    void load_graph__can_filter_by_paper_name() {
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("job_name", "Job Name");
        query.put("dataset", "mag");

        try {
            JSONArray filters = new JSONArray("[\n" +
                                              "                {\n" +
                                              "                    \"field\": \"title\",\n" +
                                              "                    \"filterType\": \"is\",\n" +
                                              "                    \"value\": \"full case study report upplandsbondens sweden\",\n" +
                                              "                    \"operator\": \"\"\n" +
                                              "                }\n" +
                                              "            ]");
            JSONObject graph = new JSONObject("{vertexType: \"Paper\"}");
            graph.put("filters", filters);
            query.put("graph", new JSONArray(graph));
            JSONObject obj = new JSONObject(query);
            TinkerGraph tg = JSON2Gremlin.load_graph(g, obj);
            assertEquals(89, Iterators.size(tg.vertices()));
            assertEquals(47, Iterators.size(tg.edges()));
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Test
    void getSubGraphForQuery_paper_in_journal() {
        JsonObject graphJson = create_json("Paper", "JournalFixed");
        TinkerGraph tg = JSON2Gremlin.getSubGraphForQuery(g, graphJson, null);
        assertEquals(2, Iterators.size(tg.vertices()));
        assertEquals(1, Iterators.size(tg.edges()));
    }

    @Test
    void getSubGraphForQuery_paper_in_conference() {
        JsonObject graphJson = create_json("Paper", "ConferenceInstance");
        TinkerGraph tg = JSON2Gremlin.getSubGraphForQuery(g, graphJson, null);
        assertEquals(2, Iterators.size(tg.vertices()));
        assertEquals(1, Iterators.size(tg.edges()));
    }

    @Test
    @Disabled
    void getSubGraphForQuery_journal_of_paper() {
        JsonObject graphJson = create_json("JournalFixed", "Paper");
        TinkerGraph tg = JSON2Gremlin.getSubGraphForQuery(g, graphJson, null);
        assertEquals(2, Iterators.size(tg.vertices()));
        assertEquals(1, Iterators.size(tg.edges()));
    }

    @Test
    void getSubGraphForQuery_author_of_paper() {
        JsonObject graphJson = create_json("Author", "Paper");
        TinkerGraph tg = JSON2Gremlin.getSubGraphForQuery(g, graphJson, null);
        assertEquals(2, Iterators.size(tg.vertices()));
        assertEquals(1, Iterators.size(tg.edges()));
    }

    @Test
    void getSubGraphForQuery_paper_or() {
        String source = "Paper";
        String target = "JournalFixed";
        String s = "{\n" +
                   "    \"job_name\": \"Job Name\",\n" +
                   "    \"dataset\": \"mag\",\n" +
                   "    \"job_id\": \"sassas1221\",\n" +
                   "    \"username\": \"abc\",\n" +
                   "    \"user_id\": \"1\",\n" +
                   "    \"graph\": {\n" +
                   "        \"nodes\": [\n" +
                   "            {\n" +
                   "                \"vertexType\": \"Paper\",\n" +
                   "                \"filters\": [\n" +
                   "                    {\n" +
                   "                        \"field\": \"paperTitle\",\n" +
                   "                        \"filterType\": \"is\",\n" +
                   "                        \"value\": \"paper2001\",\n" +
                   "                        \"operator\": \"\"\n" +
                   "                    }\n" + ",\n" +
                   "                    {\n" +
                   "                        \"field\": \"paperTitle\",\n" +
                   "                        \"filterType\": \"is\",\n" +
                   "                        \"value\": \"paper2002\",\n" +
                   "                        \"operator\": \"or\"\n" +
                   "                    }" +
                   "        ]\n" +
                   "      }], \"edges\" :  [\n" +
                   "      {\n" +
                   "                \"source\": \"" + source + "\",\n" +
                   "                \"target\": \"" + target + "\",\n" +
                   "                \"relation\": \"PublishedInRev\"\n" +
                   "            }\n" +
                   "        ]\n" +
                   "    }\n" +
                   "}";
        JsonParser jsonParser = new JsonParser();
        JsonObject messageBodyJElement = jsonParser.parse(s).getAsJsonObject();
        JsonObject graphJson = messageBodyJElement.get("graph").getAsJsonObject();
        TinkerGraph tg = JSON2Gremlin.getSubGraphForQuery(g, graphJson, null);
        assertEquals(3, Iterators.size(tg.vertices()));
        assertEquals(2, Iterators.size(tg.edges()));
    }
}
