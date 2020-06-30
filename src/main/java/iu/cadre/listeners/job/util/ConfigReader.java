package iu.cadre.listeners.job.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigReader {
    protected static final Log LOG = LogFactory.getLog(ConfigReader.class);
    private static Properties properties = new Properties();

    public static void loadProperties(String propertyFilePath) throws Exception{
        try {
            File initialFile = new File(propertyFilePath);
            InputStream propertiesStream = new FileInputStream(initialFile);
            if (propertiesStream == null) {
                LOG.error("Sorry, unable to find config.properties");
                throw new Exception("Unable to find properties file at " + propertyFilePath);
            }
            //load a properties file from class path, inside static method
            properties.load(propertiesStream);
        }
        catch (Exception e) {
            LOG.error("Error reading config propeties file.");
            throw new IOException("Error reading config file", e);
        }
    }

    public static String getAwsAccessKeyId() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.AWS_ACCSS_KEY_ID);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.AWS_ACCSS_KEY_ID);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getAwsAccessKeySecret() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.AWS_SECRET_KEY);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.AWS_SECRET_KEY);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getAwsRegion() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.AWS_REGION_NAME);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.AWS_REGION_NAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getQueueName() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.JOB_QUEUE_URL);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.JOB_QUEUE_URL);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSDBHost() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.WOS_DATABASE_HOST);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.WOS_DATABASE_HOST);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getJanusPropertiesFile() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.JANUS_PROPERTIES_FILE);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.JANUS_PROPERTIES_FILE);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getJanusHost() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.JANUS_HOST);
            }
            return "localhost";
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.JANUS_HOST);
            throw new Exception("Error reading config file", e);
        }
    }
    public static String getWOSDBPort() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.WOS_DATABASE_PORT);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.WOS_DATABASE_PORT);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSDBName() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.WOS_DATABASE_NAME);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.WOS_DATABASE_NAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSDBUsername() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.WOS_DATABASE_USERNAME);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.WOS_DATABASE_USERNAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSDBPWD() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.WOS_DATABASE_PASSWORD);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.WOS_DATABASE_PASSWORD);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSGraphDBUrl() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.WOS_GRAPH_DATABASE_URL);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.WOS_GRAPH_DATABASE_URL);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSGraphDBUsername() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.WOS_GRAPH_DATABASE_USERNAME);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.WOS_GRAPH_DATABASE_USERNAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getWOSGraphDBPwd() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.WOS_GRAPH_DATABASE_PASSWORD);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.WOS_GRAPH_DATABASE_PASSWORD);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getMetaDBHost() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.META_DATABASE_HOST);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.META_DATABASE_HOST);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getMetaDBPort() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.META_DATABASE_PORT);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.META_DATABASE_PORT);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getMetaDBName() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.META_DATABASE_NAME);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.META_DATABASE_NAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getMetaDBUsername() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.META_DATABASE_USERNAME);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.META_DATABASE_USERNAME);
            throw new Exception("Error reading config file", e);
        }
    }

    public static boolean getMetaDBInMemory() throws Exception{
        try {
            if (properties != null){
                return Boolean.valueOf(properties.getProperty(Constants.META_DATABASE_INMEMORY));
            }
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.META_DATABASE_INMEMORY);
        }
        return false;
    }

    public static String getMetaDBPWD() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.META_DATABASE_PASSWORD);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.META_DATABASE_PASSWORD);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getEFSRootListenerDir() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.EFS_ROOT_QUERY_RESULTS_DIR);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.EFS_ROOT_QUERY_RESULTS_DIR);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getEFSSubPathListenerDir() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.EFS_SUBPATH_QUERY_RESULTS_DIR);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.EFS_SUBPATH_QUERY_RESULTS_DIR);
            throw new Exception("Error reading config file", e);
        }
    }

    public static String getEFSGraphImportDir() throws Exception{
        try {
            if (properties != null){
                return properties.getProperty(Constants.EFS_ROOT_GRAPH_IMPORT_DIR);
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error reading property : " + Constants.EFS_ROOT_GRAPH_IMPORT_DIR);
            throw new Exception("Error reading config file", e);
        }
    }
}