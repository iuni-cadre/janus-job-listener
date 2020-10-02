package iu.cadre.listeners.job;

import iu.cadre.listeners.job.util.ConfigReader;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Enumeration;

public class JobStatus {
    private Connection connection;
    private PreparedStatement jobStatusPreparedStatement;
    private PreparedStatement fileInfoPreparedStatement;
    private static final Logger LOG = LoggerFactory.getLogger(JobStatus.class);

    public JobStatus(Boolean inMemory) throws Exception {
        Class.forName("org.postgresql.Driver");
        Class.forName("org.sqlite.JDBC");

        String jobUpdateStatement = "UPDATE user_job SET job_status = ?, description = ?, modified_on = CURRENT_TIMESTAMP WHERE job_id = ?";
        String fileInsertStatement = "INSERT INTO query_result(job_id,efs_path, file_checksum, data_type, authenticity, created_by, created_on) " +
                                     "VALUES(?,?,?,?,?,?,current_timestamp)";
        if (inMemory)
        {
            String metaDBUrl = "jdbc:sqlite::memory:";
            connection = DriverManager.getConnection(
                    metaDBUrl, ConfigReader.getMetaDBUsername(), ConfigReader.getMetaDBPWD());
            String sql = "CREATE TABLE IF NOT EXISTS user_job (\n"
                         + "	job_id integer PRIMARY KEY,\n"
                         + "	job_status text NOT NULL,\n"
                         + "    description character varying(256),\n"
                         + "	modified_on timestamp\n"
                         + ");";
            String sql2 = "CREATE TABLE IF NOT EXISTS query_result (\n"
                          + "   id integer PRIMARY KEY,"
                          + "	job_id text,\n"
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
            stmt.executeUpdate("INSERT INTO user_job (job_id, job_status) VALUES ('1234', 'SUBMITTED')");

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

    public void Update(String jobId, String status, String description) throws SQLException {
        LOG.info("Updating job status in metadb to " + status   );
        jobStatusPreparedStatement.setString(1, status);
        jobStatusPreparedStatement.setString(2, StringUtils.abbreviate(description, 240));
        jobStatusPreparedStatement.setString(3, jobId);
        jobStatusPreparedStatement.executeUpdate();
    }

    public void Close() throws SQLException {
        jobStatusPreparedStatement.close();
        fileInfoPreparedStatement.close();
        connection.close();
    }

    public void AddQueryResult(String jobId, String userId, String csvPath, String csvChecksum, String dataType) throws SQLException {
        fileInfoPreparedStatement.setString(1, jobId);
        fileInfoPreparedStatement.setString(2, csvPath);
        fileInfoPreparedStatement.setString(3, csvChecksum);
        fileInfoPreparedStatement.setString(4, dataType);
        fileInfoPreparedStatement.setBoolean(5, true);
        fileInfoPreparedStatement.setInt(6, Integer.parseInt(userId));
        fileInfoPreparedStatement.executeUpdate();
    }

    public String GetStatus(String job_id) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("SELECT job_status, description FROM user_job WHERE job_id = ?");
        statement.setString(1, job_id);
        ResultSet rs = statement.executeQuery();
        String result = new String();
        while (rs.next()) {
            result += rs.getString("job_status");
            result += " - ";
            result += rs.getString("description");
        }
        return result;

    }
}
