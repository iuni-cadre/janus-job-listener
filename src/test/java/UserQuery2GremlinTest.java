package iu.cadre.listeners.job;

import com.google.common.collect.Iterators;
import com.google.gson.JsonParser;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.*;
import java.util.stream.Collectors;

public class UserQuery2GremlinTest {
    static GraphTraversalSource g;

    @BeforeAll
    static void build_graph() {
        JanusGraph graph = JanusGraphFactory.build()
                .set(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory")
                .set(TIMESTAMP_PROVIDER.toStringWithoutRoot(), "NANO")
                .set(SCHEMA_CONSTRAINTS.toStringWithoutRoot(), true)
                .set(AUTO_TYPE.toStringWithoutRoot(), "none")
                .open();

        create_schema(graph);

        g = graph.traversal();

        g.addV("Paper").property("paperTitle", "full case study report upplandsbondens sweden").as("v1").
                addV("Author").property("displayName", "James W. Throckmorton").as("v2").
                addE("AuthorOf").from("v2").to("v1").
                addV("Paper").property("paperTitle", "Unicorns").as("v3").
                addE("References").from("v1").to("v3").
                iterate();

        g.addV(UserQuery2Gremlin.JOURNAL_FIELD).property("normalizedName", "the open acoustics journal").as("v1").
                addV("Paper").property("paperTitle", "coolpaper").as("v2").
                addE(UserQuery2Gremlin.PUBLISHED_IN_FIELD).from("v2").to("v1").iterate();

        for (int i = 0; i < 20; ++i)
            g.addV("Paper").property("paperTitle", String.format("Paper%s", i)).
                    property("year", "1900").as("v1").
                    addV("Author").property("displayName", "James W. Throckmorton").as("v2").
                    addE("AuthorOf").from("v2").to("v1").iterate();

        for (int i = 0; i < 20; ++i)
            g.addV("Paper").property("paperTitle", String.format("Paper%s", i + 1945)).
                    property("year", 1945f).as("v1").
                    addV("Author").property("displayName", "Elizabeth Marguerite Bowes-Lyon").as("v2").
                    addE("AuthorOf").from("v2").to("v1").iterate();

        Vertex journal = g.V().hasLabel(UserQuery2Gremlin.JOURNAL_FIELD).next();
        for (int i = 0; i < 5; ++i)
            g.addV("Paper").property("paperTitle", String.format("Paper%s", i + 2001)).
                    property("year", 2001).as("v1").
                    addE(UserQuery2Gremlin.PUBLISHED_IN_FIELD).from("v1").to(journal).iterate();
        g.tx().commit();
    }

    private static void create_schema(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        VertexLabel paper = mgmt.makeVertexLabel("Paper").make();
        VertexLabel author = mgmt.makeVertexLabel("Author").make();
        VertexLabel journal = mgmt.makeVertexLabel(UserQuery2Gremlin.JOURNAL_FIELD).make();
        EdgeLabel authorof = mgmt.makeEdgeLabel("AuthorOf").make();
        EdgeLabel publishedin = mgmt.makeEdgeLabel(UserQuery2Gremlin.PUBLISHED_IN_FIELD).make();
        EdgeLabel references = mgmt.makeEdgeLabel("References").make();
        PropertyKey title = mgmt.makePropertyKey("paperTitle").dataType(String.class).make();
        PropertyKey displayName = mgmt.makePropertyKey("displayName").dataType(String.class).make();
        PropertyKey normalizedName = mgmt.makePropertyKey("normalizedName").dataType(String.class).make();
        PropertyKey year = mgmt.makePropertyKey("year").dataType(Integer.class).make();
        mgmt.addConnection(authorof, author, paper);
        mgmt.addConnection(publishedin, paper, journal);
        mgmt.addConnection(references, paper, paper);

        mgmt.addProperties(paper, title, year);
        mgmt.addProperties(author, displayName);
        mgmt.addProperties(journal, normalizedName);
        //System.out.println(mgmt.printSchema());
        mgmt.commit();
    }

    private UserQuery create_json(final String source, final String target) {
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
                   "                \"relation\": \"" + UserQuery2Gremlin.PUBLISHED_IN_FIELD + "\"\n" +
                   "            }\n" +
                   "        ]\n" +
                   "    }\n" +
                   "}";
        JsonParser jsonParser = new JsonParser();
        return new UserQuery(jsonParser.parse(s).getAsJsonObject());
    }

    @Test
    void load_graph__throws_if_no_dataset_specified() {
        JsonParser jsonParser = new JsonParser();
        UserQuery q = new UserQuery(jsonParser.parse("{job_name: \"foo\"}").getAsJsonObject());
        assertThrows(UnsupportedOperationException.class, () -> {
            UserQuery2Gremlin.getSubGraphForQuery(g, q);
        });
    }

    @Test
    void load_graph__throws_if_not_mag_dataset() {
        String s = "{dataset: \"WOS\"}";
        JsonParser jsonParser = new JsonParser();
        UserQuery q = new UserQuery(jsonParser.parse(s).getAsJsonObject());
        assertThrows(UnsupportedOperationException.class, () -> {
            UserQuery2Gremlin.getSubGraphForQuery(g, q);
        });
    }

    @Test
    void load_graph__can_filter_by_paper_name() {
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("job_name", "Job Name");
        query.put("dataset", "mag");
        String s = "{\"csv_output\": [{\"field\": \"paperId\", \"vertexType\": \"Paper\"}, {\"field\": \"year\", \"vertexType\": \"Paper\"}, {\"field\": \"originalTitle\", \"vertexType\": \"Paper\"}, {\"field\": \"displayName\", \"vertexType\": \"Author\"}, {\"field\": \"displayName\", \"vertexType\": \"JournalFixed\"}], \"dataset\": \"mag\", \"graph\": {\"edges\": [{\"relation\": \"References\", \"source\": \"Paper\", \"target\": \"Paper\"}], \"nodes\": [{\"filters\": [{\"field\": \"title\", \"filterType\": \"is\", \"operator\": \"\", \"value\": \"Unicorns\"}], \"vertexType\": \"Paper\"}]}, \"job_id\": \"a4ed5759-4e96-4639-a7ca-389f04fb0c8a\", \"job_name\": \"TestJob\", \"user_id\": 78, \"username\": \"mjswm5lmorxw4\"}";

        JsonParser jsonParser = new JsonParser();
        UserQuery q = new UserQuery(jsonParser.parse(s).getAsJsonObject());

        TinkerGraph tg = null;
        try {
            tg = UserQuery2Gremlin.getSubGraphForQuery(g, q);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(2, Iterators.size(tg.vertices()));
        assertEquals(1, Iterators.size(tg.edges()));
    }

    @Test
    void getSubGraphForQuery_paper_in_journal() {
        UserQuery q = create_json("Paper", "JournalFixed");
        TinkerGraph tg = null;
        try {
            tg = UserQuery2Gremlin.getSubGraphForQuery(g, q);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(2, Iterators.size(tg.vertices()));
        assertEquals(1, Iterators.size(tg.edges()));
    }

    @Test
    void getSubGraphForQuery_paper_in_conference() {
        UserQuery graphJson = create_json("Paper", "ConferenceInstance");
        TinkerGraph tg = null;
        try {
            tg = UserQuery2Gremlin.getSubGraphForQuery(g, graphJson);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(2, Iterators.size(tg.vertices()));
        assertEquals(1, Iterators.size(tg.edges()));
    }

    @Test
    @Disabled
    void getSubGraphForQuery_journal_of_paper() {
        UserQuery graphJson = create_json("JournalFixed", "Paper");
        TinkerGraph tg = null;
        try {
            tg = UserQuery2Gremlin.getSubGraphForQuery(g, graphJson);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(2, Iterators.size(tg.vertices()));
        assertEquals(1, Iterators.size(tg.edges()));
    }

    @Test
    void getSubGraphForQuery_author_of_paper() {
        UserQuery graphJson = create_json("Author", "Paper");
        TinkerGraph tg = null;
        try {
            tg = UserQuery2Gremlin.getSubGraphForQuery(g, graphJson);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(2, Iterators.size(tg.vertices()));
        assertEquals(1, Iterators.size(tg.edges()));
    }

    @Test
    void JobStatus_update() {
        JobStatus js = null;
        try {
            js = new JobStatus(true);
            assertEquals("SUBMITTED - null", js.GetStatus("1234"));
            js.Update("1234", "Groovy", "Awesome");
            assertEquals("Groovy - Awesome", js.GetStatus("1234"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void JobStatus_update_maxes_at_256_chars() {
        JobStatus js = null;
        try {
            String numbers = "0123456789";
            js = new JobStatus(true);
            js.Update("1234", "Groovy", StringUtils.repeat(numbers, 30));
            assert (js.GetStatus("1234").length() < 255);
        } catch (Exception e) {
            fail(e.getMessage());
        }
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
                   "                \"relation\": \"" + UserQuery2Gremlin.PUBLISHED_IN_FIELD + "\"\n" +
                   "            }\n" +
                   "        ]\n" +
                   "    }\n" +
                   "}";
        JsonParser jsonParser = new JsonParser();
        UserQuery q = new UserQuery(jsonParser.parse(s).getAsJsonObject());
        TinkerGraph tg = null;
        try {
            tg = UserQuery2Gremlin.getSubGraphForQuery(g, q);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(3, Iterators.size(tg.vertices()));
        assertEquals(2, Iterators.size(tg.edges()));
    }

    @Test
    void getSubGraphForQuery_author_name_request_translated() {
        String s = "{\n" +
                      "    \"csv_output\": [{\n" +
                      "            \"field\": \"paperId\",\n" +
                      "            \"vertexType\": \"Paper\"\n" +
                      "        }, {\n" +
                      "            \"field\": \"year\",\n" +
                      "            \"vertexType\": \"Paper\"\n" +
                      "        }, {\n" +
                      "            \"field\": \"originalTitle\",\n" +
                      "            \"vertexType\": \"Paper\"\n" +
                      "        }, {\n" +
                      "            \"field\": \"displayName\",\n" +
                      "            \"vertexType\": \"Author\"\n" +
                      "        }, {\n" +
                      "            \"field\": \"displayName\",\n" +
                      "            \"vertexType\": \"JournalFixed\"\n" +
                      "        }\n" +
                      "    ],\n" +
                      "    \"dataset\": \"mag\",\n" +
                      "    \"graph\": {\n" +
                      "        \"edges\": [{\n" +
                      "                \"relation\": \"AuthorOf\",\n" +
                      "                \"source\": \"Author\",\n" +
                      "                \"target\": \"Paper\"\n" +
                      "            }\n" +
                      "        ],\n" +
                      "        \"nodes\": [{\n" +
                      "                \"filters\": [{\n" +
                      "                        \"field\": \"name\",\n" +
                      "                        \"filterType\": \"is\",\n" +
                      "                        \"operator\": \"\",\n" +
                      "                        \"value\": \"Throckmorton\"\n" +
                      "                    }\n" +
                      "                ],\n" +
                      "                \"vertexType\": \"Author\"\n" +
                      "            }\n" +
                      "        ]\n" +
                      "    },\n" +
                      "    \"job_id\": \"93c8ef52-d7ec-4ede-b8bb-4528bfbc1cd9\",\n" +
                      "    \"job_name\": \"athota-test4\",\n" +
                      "    \"user_id\": 82,\n" +
                      "    \"username\": \"mf2gq33ume\"\n" +
                      "}";
        JsonParser jsonParser = new JsonParser();
        UserQuery q = new UserQuery(jsonParser.parse(s).getAsJsonObject());
        TinkerGraph tg = null;
        try {
            tg = UserQuery2Gremlin.getSubGraphForQuery(g, q);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(42, Iterators.size(tg.vertices()));
        assertEquals(21, Iterators.size(tg.edges()));    }

    @Test
    void getASLabelFilters__ignores_unfiltered_nodes() {
        Node n = new Node("Paper");
        Filter f = new Filter();
        f.field = "";
        f.operator = "";
        n.filters.add(f);
        Node empty = new Node("Author");
        Map result = UserQuery2Gremlin.getASLabelFilters(Arrays.asList(n, empty));
        assertEquals(1, result.size());
    }

    @Test
    void getSubGraphForQuery__adds_AuthorOf_edge_if_none_specified() {
        String s = "{\n" +
                   "    \"csv_output\": [{\n" +
                   "            \"field\": \"paperId\",\n" +
                   "            \"vertexType\": \"Paper\"\n" +
                   "        }, {\n" +
                   "            \"field\": \"year\",\n" +
                   "            \"vertexType\": \"Paper\"\n" +
                   "        }, {\n" +
                   "            \"field\": \"originalTitle\",\n" +
                   "            \"vertexType\": \"Paper\"\n" +
                   "        }, {\n" +
                   "            \"field\": \"displayName\",\n" +
                   "            \"vertexType\": \"Author\"\n" +
                   "        }, {\n" +
                   "            \"field\": \"displayName\",\n" +
                   "            \"vertexType\": \"JournalFixed\"\n" +
                   "        }\n" +
                   "    ],\n" +
                   "    \"dataset\": \"mag\",\n" +
                   "    \"graph\": {\n" +
                   "        \"edges\": [],\n" +
                   "        \"nodes\": [{\n" +
                   "                \"filters\": [{\n" +
                   "                        \"field\": \"title\",\n" +
                   "                        \"filterType\": \"is\",\n" +
                   "                        \"operator\": \"\",\n" +
                   "                        \"value\": \"Paper8\"\n" +
                   "                    }\n" +
                   "                ],\n" +
                   "                \"vertexType\": \"Paper\"\n" +
                   "            }\n" +
                   "        ]\n" +
                   "    },\n" +
                   "    \"job_id\": \"336074bb-8071-4d5b-a448-1d0673f6f4a8\",\n" +
                   "    \"job_name\": \"\",\n" +
                   "    \"user_id\": 78,\n" +
                   "    \"username\": \"mjswm5lmorxw4\"\n" +
                   "}\n";

        JsonParser jsonParser = new JsonParser();

        UserQuery q = new UserQuery(jsonParser.parse(s).getAsJsonObject());
        TinkerGraph tg = null;
        try {
            tg = UserQuery2Gremlin.getSubGraphForQuery(g, q);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(2, Iterators.size(tg.vertices()));
        assertEquals(1, Iterators.size(tg.edges()));
    }

    @Test
    void getProjectionForQuery_returns_list_if_no_csv()
    {
        UserQuery q = mock(UserQuery.class);
        List<Node> nodes = Arrays.asList(new Node("Paper"));
        Filter f = new Filter();
        f.field = "year";
        f.value = "1945";
        nodes.get(0).filters.add(f);
        when(q.Nodes()).thenReturn(nodes);
        List actual = null;
        try {
            actual = UserQuery2Gremlin.getProjectionForQuery(g, q);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(20, actual.size());
        List titles = (List) actual.stream().map(r -> ((List)((Map)r).get("paperTitle")).get(0) ).collect( Collectors.toList() );
        titles.sort(String.CASE_INSENSITIVE_ORDER);
        assertEquals("Paper1945", titles.get(0));
    }

    @Test
    void getProjectionForQuery_returns_list_if_csv()
    {
        UserQuery q = mock(UserQuery.class);
        List<Node> nodes = Collections.singletonList(new Node("Paper"));
        Filter f = new Filter();
        f.field = "year";
        f.value = "1945";
        nodes.get(0).filters.add(f);
        List<CSVOutput> csv = Collections.singletonList(new CSVOutput());
        csv.get(0).field = "year";
        csv.get(0).vertexType = "Paper";
        when(q.Nodes()).thenReturn(nodes);
        when(q.CSV()).thenReturn(csv);
        List<Map> actual = null;
        try {
            actual = UserQuery2Gremlin.getProjectionForQuery(g, q);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(20, actual.size());
        assertEquals(1945, actual.get(0).get("Paper_year"));
    }

    @Test
    void getProjectionForQuery_returns_author_if_csv()
    {
        UserQuery q = mock(UserQuery.class);
        List<Node> nodes = Collections.singletonList(new Node("Paper"));
        nodes.get(0).filters.add(new Filter("year", "1945"));
        List<CSVOutput> csv = Arrays.asList(new CSVOutput(), new CSVOutput());
        csv.get(0).field = "year";
        csv.get(0).vertexType = "Paper";
        csv.get(1).field = "displayName";
        csv.get(1).vertexType = "Author";
        when(q.Nodes()).thenReturn(nodes);
        when(q.CSV()).thenReturn(csv);
        List<Map> actual = null;
        try {
            actual = UserQuery2Gremlin.getProjectionForQuery(g, q);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(20, actual.size());
        assertEquals(1945, actual.get(0).get("Paper_year"));
        String actualAuthor = (String)((List)actual.get(0).get("Author_displayName")).get(0);
        assertEquals("Elizabeth Marguerite Bowes-Lyon", actualAuthor);
    }

    @Test
    void getProjectionForQuery_returns_journal_if_csv()
    {
        UserQuery q = mock(UserQuery.class);
        List<Node> nodes = Collections.singletonList(new Node("Paper"));
        nodes.get(0).filters.add(new Filter("paperTitle", "Paper2003"));
        List<CSVOutput> csv = Arrays.asList(new CSVOutput(), new CSVOutput());
        csv.get(0).field = "year";
        csv.get(0).vertexType = "Paper";
        csv.get(1).field = "normalizedName";
        csv.get(1).vertexType = "JournalFixed";
        when(q.Nodes()).thenReturn(nodes);
        when(q.CSV()).thenReturn(csv);
        List<Map> actual = null;
        try {
            actual = UserQuery2Gremlin.getProjectionForQuery(g, q);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(1, actual.size());
        assertEquals(2001, actual.get(0).get("Paper_year"));
        String actualJournal = (String)((List)actual.get(0).get("JournalFixed_normalizedName")).get(0);
        assertEquals("the open acoustics journal", actualJournal);
    }

    @Test
    void getProjectionForQuery_handles_two_node_filters()
    {
        UserQuery q = mock(UserQuery.class);
        List<Node> nodes = Arrays.asList(new Node("Paper"), new Node("JournalFixed"));
        nodes.get(0).filters.add(new Filter("year", "2001"));
        nodes.get(1).filters.add(new Filter("normalizedName", "acoustics"));
        List<CSVOutput> csv = Arrays.asList(new CSVOutput(), new CSVOutput());
        csv.get(0).field = "paperTitle";
        csv.get(0).vertexType = "Paper";
        csv.get(1).field = "normalizedName";
        csv.get(1).vertexType = "JournalFixed";

        when(q.Nodes()).thenReturn(nodes);
        when(q.CSV()).thenReturn(csv);

        List<Map> actual = null;
        try {
            actual = UserQuery2Gremlin.getProjectionForQuery(g, q);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(5, actual.size());
        List titles = (List) actual.stream().map(r -> (r.get("Paper_paperTitle"))).collect( Collectors.toList() );
        titles.sort(String.CASE_INSENSITIVE_ORDER);
        assertEquals("Paper2001", titles.get(0));
    }

    @Test
    void getProjectionForQuery_returns_author()
    {
        UserQuery q = mock(UserQuery.class);
        List<Node> nodes = Collections.singletonList(new Node("Author"));
        nodes.get(0).filters.add(new Filter("displayName", "Throckmorton"));
        List<CSVOutput> csv = Arrays.asList(new CSVOutput(), new CSVOutput());
        csv.get(0).field = "paperTitle";
        csv.get(0).vertexType = "Paper";
        csv.get(1).field = "displayName";
        csv.get(1).vertexType = "Author";
        when(q.Nodes()).thenReturn(nodes);
        when(q.CSV()).thenReturn(csv);
        List<Map> actual = null;
        try {
            actual = UserQuery2Gremlin.getProjectionForQuery(g, q);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertEquals(21, actual.size());
        List titles = (List) actual.stream().map(r -> (((List)r.get("Paper_paperTitle")).get(0))).sorted().collect( Collectors.toList() );
        assertEquals("Paper0", titles.get(0));
        assertEquals("full case study report upplandsbondens sweden", titles.get(20));
    }

}
