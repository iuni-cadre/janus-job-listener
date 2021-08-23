package iu.cadre.listeners.job;

import iu.cadre.listeners.job.util.ConfigReader;
import iu.cadre.listeners.job.JobListenerInterruptHandler;
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
    private static final String updateAsStoppedStatement = "UPDATE listener_status SET status = ?, last_report_time = current_timestamp WHERE listener_id = ?";
    public static final String CLUSTER_NONE = "NONE";
    public static final String CLUSTER_WOS = "WOS";
    public static final String CLUSTER_MAG = "MAG";
    public static final String CLUSTER_USPTO = "USPTO";
    public static final String STATUS_STOPPED = "STOPPED";
    public static final String STATUS_IDLE = "IDLE";
    public static final String STATUS_RUNNING = "RUNNING";
    public static final int CONNECTION_VALIDATION_TIMEOUT = 5; // sec

    private ReentrantLock mutex;
    private boolean shuttingDown;
    private String metaDBUrl;
    private Connection connection;
    private PreparedStatement statusUpdatePreparedStatement;
    private PreparedStatement updateAsStoppedPreparedStatement;
    private int listenerID;

    public ListenerStatus(int listenerID, Boolean inMemory) throws Exception {
        Class.forName("org.postgresql.Driver");
        Class.forName("org.sqlite.JDBC");

        this.listenerID = listenerID;
        shuttingDown = false;
        mutex = new ReentrantLock();

        if (inMemory)
        {
            metaDBUrl = "jdbc:sqlite::memory:";
            createConnection();
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
            metaDBUrl = "jdbc:postgresql://" + ConfigReader.getMetaDBHost() + ":" +
                               ConfigReader.getMetaDBPort() + "/" + ConfigReader.getMetaDBName();
            createConnection();
        }
    }

    private void createConnection() throws SQLException,Exception {
       connection = DriverManager.getConnection(metaDBUrl, ConfigReader.getMetaDBUsername(),
                    ConfigReader.getMetaDBPWD());
       statusUpdatePreparedStatement = connection.prepareStatement(statusUpdateStatement);
       updateAsStoppedPreparedStatement = connection.prepareStatement(updateAsStoppedStatement);
    }

    public void refreshConnection() throws SQLException,Exception {
       try {
          mutex.lock();
          if (!connection.isValid(ListenerStatus.CONNECTION_VALIDATION_TIMEOUT)) {
            close();
            createConnection();
          }
       } finally {
          mutex.unlock();
       }
    }

    private void close() throws SQLException {
       statusUpdatePreparedStatement.close();
       updateAsStoppedPreparedStatement.close();
       connection.close();
    }

    public void update(String newCluster, String newStatus) throws SQLException,Exception {
        if (!newStatus.contentEquals(STATUS_STOPPED) &&
            !newStatus.contentEquals(STATUS_IDLE) &&
            !newStatus.contentEquals(STATUS_RUNNING)) {
           throw new Exception("Invalid status given -- " + newStatus);
        }

        try {
           mutex.lock();
           // If the interrupt handler is being called, don't update
           if (!shuttingDown) {
              statusUpdatePreparedStatement.setString(1, newCluster);
              statusUpdatePreparedStatement.setString(2, newStatus);
              statusUpdatePreparedStatement.setInt(3, listenerID);
              statusUpdatePreparedStatement.executeUpdate();
              LOG.info("Updated status of listener " + listenerID + " to " + newStatus);
           }
        } finally {
           mutex.unlock();
        }
    }

    // The object is assumed not to be used anymore after this function is called
    public void updateAsStopped() throws SQLException {
        try {
           mutex.lock();
           shuttingDown = true;
           updateAsStoppedPreparedStatement.setString(1, ListenerStatus.STATUS_STOPPED);
           updateAsStoppedPreparedStatement.setInt(2, listenerID);
           updateAsStoppedPreparedStatement.executeUpdate();
           LOG.info("Updated status of listener " + listenerID + " to " + ListenerStatus.STATUS_STOPPED);
        } finally {
           mutex.unlock();
        }
    }

    public void finalize() {
       try {
          close();
       } catch (Exception e) {
          ;
       }
    }
}
