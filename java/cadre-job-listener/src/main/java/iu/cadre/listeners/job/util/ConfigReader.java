package iu.cadre.listeners.job.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigReader {
    protected static final Log LOG = LogFactory.getLog(ConfigReader.class);

    public static Properties loadProperties() throws Exception{
        try (InputStream input = ConfigReader.class.getClassLoader().getResourceAsStream("cadre_config.properties")) {

            Properties prop = new Properties();

            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return null;
            }
            //load a properties file from class path, inside static method
            prop.load(input);
            return prop;

        } catch (IOException e) {
            LOG.error("Error reading config propeties file.");
            throw new IOException("Error reading config file", e);
        }
    }

    public static String getAwsAccessKeyId() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.AWS_ACCSS_KEY_ID);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.AWS_ACCSS_KEY_ID);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getAwsAccessKeySecret() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.AWS_SECRET_KEY);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.AWS_SECRET_KEY);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getAwsRegion() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.AWS_REGION_NAME);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.AWS_REGION_NAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getQueueName() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.JOB_QUEUE_URL);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.JOB_QUEUE_URL);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSDBHost() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.WOS_DATABASE_HOST);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.WOS_DATABASE_HOST);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getJanusPropertiesFile() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.JANUS_PROPERTIES_FILE);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.JANUS_PROPERTIES_FILE);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSDBPort() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.WOS_DATABASE_PORT);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.WOS_DATABASE_PORT);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSDBName() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.WOS_DATABASE_NAME);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.WOS_DATABASE_NAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSDBUsername() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.WOS_DATABASE_USERNAME);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.WOS_DATABASE_USERNAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSDBPWD() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.WOS_DATABASE_PASSWORD);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.WOS_DATABASE_PASSWORD);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSGraphDBUrl() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.WOS_GRAPH_DATABASE_URL);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.WOS_GRAPH_DATABASE_URL);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSGraphDBUsername() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.WOS_GRAPH_DATABASE_USERNAME);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.WOS_GRAPH_DATABASE_USERNAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSGraphDBPwd() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.WOS_GRAPH_DATABASE_PASSWORD);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.WOS_GRAPH_DATABASE_PASSWORD);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getMetaDBHost() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.META_DATABASE_HOST);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.META_DATABASE_HOST);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getMetaDBPort() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.META_DATABASE_PORT);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.META_DATABASE_PORT);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getMetaDBName() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.META_DATABASE_NAME);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.META_DATABASE_NAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getMetaDBUsername() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.META_DATABASE_USERNAME);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.META_DATABASE_USERNAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getMetaDBPWD() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.META_DATABASE_PASSWORD);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.META_DATABASE_PASSWORD);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getEFSRootListenerDir() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.EFS_ROOT_QUERY_RESULTS_DIR);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.EFS_ROOT_QUERY_RESULTS_DIR);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getEFSSubPathListenerDir() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.EFS_SUBPATH_QUERY_RESULTS_DIR);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.EFS_SUBPATH_QUERY_RESULTS_DIR);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getEFSGraphImportDir() throws Exception{
        try {
            Properties properties = loadProperties();
            if (properties != null){
                return properties.getProperty(Constants.EFS_ROOT_GRAPH_IMPORT_DIR);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Error reading property : " + Constants.EFS_ROOT_GRAPH_IMPORT_DIR);
            throw new Exception("Error reading config file", e);
        }
    }
}