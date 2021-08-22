package iu.cadre.listeners.job;

import iu.cadre.listeners.job.util.ConfigReader;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Enumeration;
import java.util.concurrent.locks.*;
import java.lang.Runnable;
import java.lang.Thread;


public class ListenerStatus {
    private static final Logger LOG = LoggerFactory.getLogger(ListenerStatus.class);
    private static final String statusUpdateStatement = "UPDATE listener_status SET last_cluster = ?, status = ?, last_report_time = current_timestamp WHERE listener_id = ?";
    public static final String CLUSTER_NONE = "NONE";
    public static final String CLUSTER_WOS = "WOS";
    public static final String CLUSTER_MAG = "MAG";
    public static final String CLUSTER_USPTO = "USPTO";
    public static final String STATUS_STOPPED = "STOPPED";
    public static final String STATUS_IDLE = "IDLE";
    public static final String STATUS_RUNNING = "RUNNING";

    private Connection connection;
    private PreparedStatement statusUpdatePreparedStatement;
    private int listenerID;

    public ListenerStatus(int listenerID, Boolean inMemory) throws Exception {
        Class.forName("org.postgresql.Driver");
        Class.forName("org.sqlite.JDBC");

        this.listenerID = listenerID;

        if (inMemory)
        {
            String metaDBUrl = "jdbc:sqlite::memory:";
            connection = DriverManager.getConnection(
                    metaDBUrl, ConfigReader.getMetaDBUsername(), ConfigReader.getMetaDBPWD());
            String sql = "CREATE TABLE IF NOT EXISTS listener_status (\n"
                         + "	listener_id int PRIMARY KEY,\n"
                         + "    last_cluster varchar(32),\n"
                         + "	status varchar(32) CHECK (status IN ('IDLE', 'RUNNING', 'STOPPED')),\n"
                         + "	last_report_time timestamp);";

            Statement stmt = connection.createStatement();
            // create a new table
            stmt.execute(sql);

            for (int i=0; i < 16; i++) {
               statusUpdatePreparedStatement.setString(1, ListenerStatus.CLUSTER_NONE);
               statusUpdatePreparedStatement.setString(2, ListenerStatus.STATUS_STOPPED);
               statusUpdatePreparedStatement.setInt(3, i);
               statusUpdatePreparedStatement.executeUpdate();
            }
        }
        else
        {
            String metaDBUrl = "jdbc:postgresql://" + ConfigReader.getMetaDBHost() + ":" +
                               ConfigReader.getMetaDBPort() + "/" + ConfigReader.getMetaDBName();
            connection = DriverManager.getConnection(metaDBUrl, ConfigReader.getMetaDBUsername(),
                    ConfigReader.getMetaDBPWD());
        }

        statusUpdatePreparedStatement = connection.prepareStatement(statusUpdateStatement);
    }

    public void close() throws SQLException {
       statusUpdatePreparedStatement.close();
       connection.close();
    }

    public void update(String newCluster, String newStatus) throws Exception {
        if (!newStatus.contentEquals(STATUS_STOPPED) &&
            !newStatus.contentEquals(STATUS_IDLE) &&
            !newStatus.contentEquals(STATUS_RUNNING)) {
           throw new Exception("Invalid status given -- " + newStatus);
        }

        statusUpdatePreparedStatement.setString(1, newCluster);
        statusUpdatePreparedStatement.setString(2, newStatus);
        statusUpdatePreparedStatement.setInt(3, listenerID);
        statusUpdatePreparedStatement.executeUpdate();
        LOG.info("Updated status of listener " + listenerID + " to " + newStatus);
    }

}
