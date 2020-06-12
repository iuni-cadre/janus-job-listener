package iu.cadre.listeners.job.util;

import java.util.HashMap;
import java.util.Map;

public class Constants {
    public static final String AWS_ACCSS_KEY_ID = "aws.access.key.id";
    public static final String AWS_SECRET_KEY = "aws.secret.access.key";
    public static final String AWS_REGION_NAME = "region.name";
    public static final String JOB_QUEUE_URL = "job.queue";
    public static final String JANUS_PROPERTIES_FILE = "janus.config";
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

    public static final Map<String, String> vertexLableMap = new HashMap<String, String>();
}
