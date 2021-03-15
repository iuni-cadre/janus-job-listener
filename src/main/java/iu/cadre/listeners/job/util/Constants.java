package iu.cadre.listeners.job.util;

import iu.cadre.listeners.job.UserQuery2Gremlin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Constants {
    public static final String AWS_ACCSS_KEY_ID = "aws.access.key.id";
    public static final String AWS_SECRET_KEY = "aws.secret.access.key";
    public static final String AWS_REGION_NAME = "region.name";
    public static final String JOB_QUEUE_URL = "job.queue";
    public static final String JANUS_MAG_PROPERTIES_FILE = "janus.mag.config";
    public static final String JANUS_WOS_PROPERTIES_FILE = "janus.wos.config";
    public static final String JANUS_USPTO_PROPERTIES_FILE = "janus.uspto.config";
    public static final String JANUS_HOST = "janus.host";
    public static final String JANUS_RECORD_LIMIT = "janus.recordlimit";
    public static final String WOS_DATABASE_HOST = "wos.database.host";
    public static final String WOS_DATABASE_PORT = "wos.database.port";
    public static final String WOS_DATABASE_NAME = "wos.database.name";
    public static final String WOS_DATABASE_USERNAME = "wos.database.username";
    public static final String WOS_DATABASE_PASSWORD = "wos.database.password";
    public static final String WOS_GRAPH_DATABASE_URL = "wos.graph.database.url";
    public static final String WOS_GRAPH_DATABASE_USERNAME = "wos.graph.database.username";
    public static final String WOS_GRAPH_DATABASE_PASSWORD = "wos.graph.database.password";
    public static final String WOS_GRAPH_IMPORT_DIR = "wos.graph.import.dir";
    public static final String META_DATABASE_HOST = "meta.database.host";
    public static final String META_DATABASE_PORT = "meta.database.port";
    public static final String META_DATABASE_NAME = "meta.database.name";
    public static final String META_DATABASE_USERNAME = "meta.database.username";
    public static final String META_DATABASE_PASSWORD = "meta.database.password";
    public static final String META_DATABASE_INMEMORY = "meta.database.inmemory";
    public static final String EFS_ROOT_QUERY_RESULTS_DIR = "efs.root.query.results.listener";
    public static final String EFS_SUBPATH_QUERY_RESULTS_DIR = "efs.subpath.query.results.listener";
    public static final String EFS_ROOT_GRAPH_IMPORT_DIR = "efs.root.graph.import.listener";
    public static final String USPTO_DATE_FORMAT = "EEE LLL dd HH:mm:ss zzz";

    public static final Map<String, String> vertexLableMap = new HashMap<String, String>();

    // Vertices and edges for MAG and WOS
    public static final String PAPER_FIELD = "Paper";
    public static final String JOURNAL_FIELD = "JournalFixed";
    public static final String PUBLISHED_IN_FIELD = "PublishedInFixed";
    public static final String AUTHOR_FIELD = "Author";
    public static final String AUTHOR_OF_FIELD = "AuthorOf";
    public static final String CONFERENCE_INSTANCE_FIELD = "ConferenceInstance";
    public static final String PRESENTED_AT_FIELD = "PresentedAt";
    public static final String FIELD_OF_STUDY_FIELD = "FieldOfStudy";
    public static final String BELONGS_TO_FIELD = "BelongsTo";
    public static final String AFFILIATION_FIELD = "Affiliation";
    public static final String AFFILIATED__WITH_FIELD = "AffiliatedWith";

    // Vertices and Edges for USPTO
    public static final String PATENT_FIELD = "Patent";
    public static final String INVENTOR_FIELD = "Inventor";
    public static final String INVENTOR_OF_FIELD = "Inventor_Of";
    public static final String LOCATION_FIELD = "Location";
    public static final String INVENTOR_LOCATED_IN_FIELD = "Inventor_Located_In";
    public static final String USPC_FIELD = "USPC";
    public static final String USPC_CATEGORY_OF_FIELD = "USPC_Category_Of";
    public static final String CPC_FIELD = "CPC";
    public static final String CPC_CATEGORY_OF_FIELD = "CPC_Category_Of";
    public static final String ASSIGNEE_FIELD = "Assignee";
    public static final String ASSIGN_TO_FIELD = "Assigned_To";
}
