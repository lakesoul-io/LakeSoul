package com.dmetasoul.lakesoul.meta.dao;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.entity.StreamingRecordInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class StreamingRecordInfoDao {
    public StreamingRecordInfo selectByTableId(String tableId) {
        String sql = String.format("select * from streaming_record_info where table_id = '%s'", tableId);
        return getStreamingRecordInfo(sql);
    }

    public StreamingRecordInfo selectByTableIdAndQueryId(String tableId, String queryId) {
        String sql = String.format("select * from streaming_record_info where table_id = '%s' and query_id = '%s'", tableId, queryId);
        return getStreamingRecordInfo(sql);
    }

    public List<StreamingRecordInfo> selectByTableIdAndTimeStamp(String tableId, long timestamp) {
        String sql = String.format("select * from streaming_record_info where table_id = '%s' and timestamp < %d", tableId, timestamp);
        return getStreamingRecordInfos(sql);
    }

    public void updateStreamingRecordInfo(String tableId, String queryId, long batchId, long timestamp) {
        insertStreamingRecordInfo(new StreamingRecordInfo(tableId, queryId, batchId, timestamp));
    }

    private boolean insertStreamingRecordInfo(StreamingRecordInfo streamingRecordInfo) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        boolean result = true;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into streaming_record_info (table_id, query_id, batch_id,timestamp) values (?, ?, ?,?)");
            pstmt.setString(1, streamingRecordInfo.getTableId());
            pstmt.setString(2, streamingRecordInfo.getQueryId());
            pstmt.setLong(3, streamingRecordInfo.getBatchId());
            pstmt.setLong(4, streamingRecordInfo.getTimestamp());
            pstmt.execute();
        } catch (SQLException e) {
            result = false;
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
        return result;
    }

    public void deleteStreamingRecordInfoByTableId(String tableId) {
        String sql = String.format("delete from streaming_record_info where table_id = '%s'", tableId);
        deleteStreamingRecordInfo(sql);
    }

    public void deleteStreamingRecordInfoByTableIdAndQueryId(String tableId, String queryId) {
        String sql = String.format("delete from streaming_record_info where table_id = '%s' and query_id = '%s'", tableId, queryId);
        deleteStreamingRecordInfo(sql);
    }

    private void deleteStreamingRecordInfo(String sql) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    private StreamingRecordInfo getStreamingRecordInfo(String sql) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        StreamingRecordInfo streamingRecordInfo = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                streamingRecordInfo = new StreamingRecordInfo();
                streamingRecordInfo.setTableId(rs.getString("table_id"));
                streamingRecordInfo.setQueryId(rs.getString("query_id"));
                streamingRecordInfo.setBatchId(rs.getLong("batch_id"));
                streamingRecordInfo.setTimestamp(rs.getLong("timestamp"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return streamingRecordInfo;
    }

    private List<StreamingRecordInfo> getStreamingRecordInfos(String sql) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        StreamingRecordInfo streamingRecordInfo = null;
        List<StreamingRecordInfo> streamingRecords = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                streamingRecordInfo = new StreamingRecordInfo();
                streamingRecordInfo.setTableId(rs.getString("table_id"));
                streamingRecordInfo.setQueryId(rs.getString("query_id"));
                streamingRecordInfo.setBatchId(rs.getLong("batch_id"));
                streamingRecordInfo.setTimestamp(rs.getLong("timestamp"));
                streamingRecords.add(streamingRecordInfo);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return streamingRecords;
    }
}
