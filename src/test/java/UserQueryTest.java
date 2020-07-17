package iu.cadre.listeners.job;

import com.google.gson.JsonParser;
import iu.cadre.listeners.job.UserQuery;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class UserQueryTest {

    @Test
    void UserQuery_can_read_edges_and_nodes_from_JSON() {
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
                   "                \"source\": \"mysource\",\n" +
                   "                \"target\": \"mytarget\",\n" +
                   "                \"relation\": \"PublishedIn\"\n" +
                   "            }\n" +
                   "        ]\n" +
                   "    }\n" +
                   "}";
        JsonParser jsonParser = new JsonParser();
        UserQuery q = new UserQuery(jsonParser.parse(s).getAsJsonObject());
        assertEquals(1, q.Edges().size());
        assertEquals(1, q.Nodes().size());
        assertEquals(2, q.Nodes().get(0).filters.size());
    }

    @Test
    void UserQuery_can_read_csv_from_JSON() {
        String s = "{\n" +
                   "  \"csv_output\": [{\n" +
                   "    \"field\": \"paperId\",\n" +
                   "    \"vertexType\": \"Paper\"\n" +
                   "  }, {\n" +
                   "    \"field\": \"year\",\n" +
                   "    \"vertexType\": \"Paper\"\n" +
                   "  }, {\n" +
                   "    \"field\": \"originalTitle\",\n" +
                   "    \"vertexType\": \"Paper\"\n" +
                   "  }, {\n" +
                   "    \"field\": \"displayName\",\n" +
                   "    \"vertexType\": \"Author\"\n" +
                   "  }, {\n" +
                   "    \"field\": \"displayName\",\n" +
                   "    \"vertexType\": \"JournalFixed\"\n" +
                   "  }\n" +
                   "  ],\n" +
                   "  \"dataset\": \"mag\"}";
        JsonParser jsonParser = new JsonParser();
        UserQuery q = new UserQuery(jsonParser.parse(s).getAsJsonObject());
        assertEquals(5, q.CSV().size());
        assertEquals("mag", q.DataSet());
    }

    @Test
    void UserQuery_requires_graph_if_references_edge_found() {
        String s = "  {\"graph\": {\n" +
                   "    \"edges\": [{\n" +
                   "      \"relation\": \"References\",\n" +
                   "      \"source\": \"Paper\",\n" +
                   "      \"target\": \"Paper\"\n" +
                   "    }]}}";
        JsonParser jsonParser = new JsonParser();
        UserQuery q = new UserQuery(jsonParser.parse(s).getAsJsonObject());
        assertTrue(q.RequiresGraph());
    }

    @Test
    void UserQuery_does_not_require_graph_if_references_edge_not_found() {
        String s = "  {\"graph\": {\n" +
                   "    \"edges\": [{\n" +
                   "      \"relation\": \"AuthorOf\",\n" +
                   "      \"source\": \"Author\",\n" +
                   "      \"target\": \"Paper\"\n" +
                   "    }]}}";
        JsonParser jsonParser = new JsonParser();
        UserQuery q = new UserQuery(jsonParser.parse(s).getAsJsonObject());
        assertFalse(q.RequiresGraph());
    }
}