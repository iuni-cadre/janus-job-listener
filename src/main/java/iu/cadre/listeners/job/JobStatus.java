package iu.cadre.listeners.job;

import iu.cadre.listeners.job.util.ConfigReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

class JobStatus {
    private Connection connection;
    private PreparedStatement jobStatusPreparedStatement;
    private PreparedStatement fileInfoPreparedStatement;
    private static final Logger LOG = LoggerFactory.getLogger(JobStatus.class);

    JobStatus() throws Exception {
        String jobUpdateStatement = "UPDATE user_job SET job_status = ?, modified_on = CURRENT_TIMESTAMP WHERE job_id = ?";
        String fileInsertStatement = "INSERT INTO query_result(job_id,efs_path, file_checksum, data_type, authenticity, created_by, created_on) " +
                                     "VALUES(?,?,?,?,?,?,current_timestamp)";
        if (ConfigReader.getMetaDBInMemory())
        {
            String metaDBUrl = "jdbc:sqlite::memory:";
            connection = DriverManager.getConnection(
                    metaDBUrl, ConfigReader.getMetaDBUsername(), ConfigReader.getMetaDBPWD());
            String sql = "CREATE TABLE IF NOT EXISTS user_job (\n"
                         + "	job_id integer PRIMARY KEY,\n"
                         + "	job_status text NOT NULL,\n"
                         + "	modified_on timestamp\n"
                         + ");";
            String sql2 = "CREATE TABLE IF NOT EXISTS query_result (\n"
                          + "	job_id integer PRIMARY KEY,\n"
                          + "	efs_path text NOT NULL,\n"
                          + "	file_checksum text NOT NULL,\n"
                          + "	data_type text NOT NULL,\n"
                          + "	authenticity boolean NOT NULL,\n"
                          + "	created_by integer NOT NULL,\n"
                          + "	created_on timestamp\n"
                          + ");";

            Statement stmt = connection.createStatement();
            // create a new table
            stmt.execute(sql);
            stmt.execute(sql2);
        }
        else
        {
            String metaDBUrl = "jdbc:postgresql://" + ConfigReader.getMetaDBHost() + ":" +
                               ConfigReader.getMetaDBPort() + "/" + ConfigReader.getMetaDBName();
            connection = DriverManager.getConnection(metaDBUrl, ConfigReader.getMetaDBUsername(),
                    ConfigReader.getMetaDBPWD());
        }

        jobStatusPreparedStatement = connection.prepareStatement(jobUpdateStatement);
        fileInfoPreparedStatement = connection.prepareStatement(fileInsertStatement);

    }

    public void Update(String jobId, String status) throws SQLException {
        LOG.info("Updating job status in metadb to " + status   );
        jobStatusPreparedStatement.setString(1, status);
        jobStatusPreparedStatement.setString(2, jobId);
        jobStatusPreparedStatement.executeUpdate();
    }

    public void Close() throws SQLException {
        jobStatusPreparedStatement.close();
        fileInfoPreparedStatement.close();
        connection.close();
    }

    public void AddQueryResult(String jobId, String userId, String csvPath, String csvChecksum) throws SQLException {
        fileInfoPreparedStatement.setString(1, jobId);
        fileInfoPreparedStatement.setString(2, csvPath);
        fileInfoPreparedStatement.setString(3, csvChecksum);
        fileInfoPreparedStatement.setString(4, "MAG");
        fileInfoPreparedStatement.setBoolean(5, true);
        fileInfoPreparedStatement.setInt(6, Integer.parseInt(userId));
        fileInfoPreparedStatement.executeUpdate();
    }
}
