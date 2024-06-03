package com.testdb;

import org.apache.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.*;

/**
 * 用Java获取PostgreSQL变更数据
 * @Author: Grainger
 * @Date: 2024-06-03 15:25
 * @E-mail: sdfiyon@gmail.com
 */
public class PgReplicationTest {
    private static Logger logger;
    private PostgresConnection conn;
    protected PGConnection replConnection;
    protected PGReplicationStream stream;
    protected String startLSN;

    static {
        PgReplicationTest.logger = Logger.getLogger(PgReplicationTest.class);
    }

    public PgReplicationTest() {
        this.conn = new PostgresConnection();
    }

    /**
     * 获取开始的LSN
     * @return
     * @throws SQLException
     */
    public LogSequenceNumber getStartLSN() throws SQLException {
        LogSequenceNumber currentLSN = null;
        if (startLSN == null) {
            try {
                final String lsn = this.queryforLSN("SELECT pg_current_xlog_location();", "pg_current_xlog_location");
                currentLSN = LogSequenceNumber.valueOf(lsn);
            }
            catch (SQLException exFunction) {
                if (!exFunction.getSQLState().equals("42883")) {
                    logger.error(exFunction);
                    throw exFunction;
                }
                final String lsn2 = this.queryforLSN("SELECT pg_current_wal_lsn();", "pg_current_wal_lsn");
                currentLSN = LogSequenceNumber.valueOf(lsn2);
            }
        }
        else {
            currentLSN = LogSequenceNumber.valueOf(startLSN);
        }
        logger.warn("Start LSN : " + currentLSN);
        return currentLSN;
    }

    /**
     * 查询LSN
     * @param query
     * @param column
     * @return
     * @throws SQLException
     */
    private String queryforLSN(final String query, final String column) throws SQLException {
        Statement getCurrentLSNStmt = null;
        ResultSet rsCurrentLSN = null;
        String lsn = null;
        getCurrentLSNStmt = this.conn.getConnection().createStatement();
        rsCurrentLSN = getCurrentLSNStmt.executeQuery(query);
        try {
            if (rsCurrentLSN != null) {
                while (rsCurrentLSN.next()) {
                    logger.warn(" Getting current LSN using query : " + query);
                    lsn = rsCurrentLSN.getString(column);
                }
            }
        }
        finally {
            if (rsCurrentLSN != null) {
                try {
                    rsCurrentLSN.close();
                }
                catch (SQLException ex) {}
            }
            if (getCurrentLSNStmt != null) {
                try {
                    getCurrentLSNStmt.close();
                }
                catch (SQLException ex2) {}
            }
            this.conn.closeConnection();
        }
        return lsn;
    }

    /**
     * 启动复制槽
     * @param slotName
     * @throws Exception
     */
    private void startSlot(String slotName) throws Exception {
        final LogSequenceNumber startLSN = this.getStartLSN();
        this.replConnection = this.conn.getReplicationConnection();
        this.stream = (
                (this.replConnection.getReplicationAPI().replicationStream().logical().withSlotName(slotName))
                        .withSlotOption("include-xids", true)
                        .withSlotOption("include-timestamp", true)
                        .withStartPosition(startLSN)
        )
        .start();
        logger.warn("PostgreSQL Reader is successfully Positioned at :" + startLSN);
    }

    /**
     * 创建复制槽
     * @param slotName
     * @throws Exception
     */
    private void createSlot(String slotName) throws Exception{
        String sql = "SELECT slot_name, plugin FROM pg_replication_slots WHERE slot_name = ?;";
        String createSlotSql = "SELECT * FROM pg_create_logical_replication_slot('"+slotName+"', 'test_decoding');";
        Connection connection = this.conn.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, slotName);
        ResultSet rs = preparedStatement.executeQuery();
        if(!rs.next()){
            Statement statement = connection.createStatement();
            statement.executeQuery(createSlotSql);
            statement.close();
        }
        rs.close();
        preparedStatement.close();
        connection.close();
    }

    /**
     * 获取操作记录
     * @return
     * @throws Exception
     */
    public String processRecords() throws Exception {
        String event = null;
        final ByteBuffer record = this.stream.read();
        if (record != null) {
            final int offset = record.arrayOffset();
            final byte[] source = record.array();
            final int length = source.length - offset;
            event = new String(source, offset, length);
        }
        return event;
    }


    public static void main(String[] args) throws Exception {
        String slotName = "test_slot";//复制槽名称
        PgReplicationTest test = new PgReplicationTest();
        test.createSlot(slotName);
        test.startSlot(slotName);
        while(true) {
            System.out.println(test.processRecords());//循环获取变更数据
        }
    }


}
