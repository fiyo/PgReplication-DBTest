package com.testdb;

import org.apache.log4j.*;
import java.util.*;
import org.postgresql.*;
import java.sql.*;

/**
 * 数据库连接管理类
 * @Author: Grainger
 * @Date: 2024-06-03 16:07
 * @E-mail: sdfiyon@gmail.com
 */
public class PostgresConnection
{
    private static Logger logger;
    Connection dbConnection;
    PGConnection replConnection;

    private static String URL = "jdbc:postgresql://localhost:5432/postgres";
    private static String USERNAME = "repuser";
    private static String PASSWORD = "repuser";


    public PostgresConnection() {
        this.replConnection = null;
    }

    public Connection getConnection() throws SQLException {
        Connection tempConnObj = null;
        final Properties connProperties = new Properties();
        PGProperty.USER.set(connProperties, USERNAME);
        PGProperty.PASSWORD.set(connProperties, PASSWORD);
        try {
                if (this.dbConnection == null || this.dbConnection.isClosed()) {
                    PGProperty.ASSUME_MIN_SERVER_VERSION.set(connProperties, "9.4");
                    PGProperty.REPLICATION.set(connProperties, "database");
                    PGProperty.PREFER_QUERY_MODE.set(connProperties, "simple");
                    this.dbConnection = DriverManager.getConnection(URL, connProperties);
                }
                tempConnObj = this.dbConnection;

        }
        catch (SQLException exSQL) {
            if (logger.isDebugEnabled()) {
                logger.debug("Exception occured while getting connection ", exSQL);
            }
            throw exSQL;
        }
        return tempConnObj;
    }

    public PGConnection getReplicationConnection() throws SQLException {
        try {
            this.getConnection();
            if (this.dbConnection != null) {
                this.replConnection = this.dbConnection.unwrap(PGConnection.class);
            }
        }
        catch (SQLException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Not able to obtain replication connection", e);
            }
            throw e;
        }
        return this.replConnection;
    }

    public void closeConnection() {
        if (this.dbConnection != null) {
            try {
                this.dbConnection.close();
                this.dbConnection = null;
            }
            catch (SQLException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Exception occured while closing the replication connection", e);
                }
            }
        }
    }

    static {
        logger = Logger.getLogger(PostgresConnection.class);
    }
}