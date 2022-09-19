package com.dmetasoul.lakesoul.meta.dao;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.Namespace;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class NamespaceDao {
    public boolean insert(Namespace namespace) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        boolean result = true;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into namespace(namespace, properties, comment) " +
                    "values (?, ?, ?)");
            pstmt.setString(1, namespace.getNamespace());
            pstmt.setString(2, DBUtil.jsonToString(namespace.getProperties()));
            pstmt.setString(3, namespace.getComment());
            pstmt.execute();
        } catch (SQLException e) {
            result = false;
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
        return result;
    }

    public Namespace findByNamespace(String name) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select * from namespace where namespace = '%s'", name);
        Namespace namespace = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                namespace = new Namespace();
                namespace.setNamespace(rs.getString("namespace"));
                namespace.setProperties(DBUtil.stringToJSON(rs.getString("properties")));
                namespace.setComment(rs.getString("comment"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return namespace;
    }

    public void deleteByNamespace(String namespace) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("delete from namespace where namespace = '%s' ", namespace);
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

    public List<String> listNamespace() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select namespace from namespace");
        List<String> list = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String tablePath = rs.getString("namespace");
                list.add(tablePath);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public int updatePropertiesByNamespace(String namespace, JSONObject properties) {
        int result = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;
        StringBuilder sb = new StringBuilder();
        sb.append("update namespace set ");
        sb.append(String.format("properties = '%s'", properties.toJSONString()));
        sb.append(String.format(" where namespace = '%s'", namespace));
        System.out.println(sb.toString());
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sb.toString());
            result = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
        return result;
    }

    public void clean() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("delete from namespace;");
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }
}
