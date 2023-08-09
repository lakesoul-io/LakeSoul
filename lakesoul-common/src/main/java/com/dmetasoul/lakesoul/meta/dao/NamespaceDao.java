// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.dao;

import com.dmetasoul.lakesoul.meta.DBConfig;
import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.dmetasoul.lakesoul.meta.entity.Namespace;
import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;
import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;
import com.dmetasoul.lakesoul.meta.rbac.AuthZContext;
import com.dmetasoul.lakesoul.meta.rbac.AuthZEnforcer;
import dev.failsafe.internal.util.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class NamespaceDao {
    public void insert(Namespace namespace) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.insert(
                    NativeUtils.CodedDaoType.InsertNamespace,
                    JniWrapper.newBuilder().addNamespace(namespace).build());
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into namespace(namespace, properties, comment, domain) " +
                    "values (?, ?, ?, ?)");
            pstmt.setString(1, namespace.getNamespace());
            pstmt.setString(2, namespace.getProperties());
            pstmt.setString(3, namespace.getComment());
            pstmt.setString(4, namespace.getDomain());
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public Namespace findByNamespace(String name) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectNamespaceByNamespace,
                    Collections.singletonList(name));
            if (jniWrapper == null) return null;
            List<Namespace> namespaceList = jniWrapper.getNamespaceList();
            return namespaceList.isEmpty() ? null : namespaceList.get(0);
        }
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
                namespace = namespaceFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return namespace;
    }

    public void deleteByNamespace(String namespace) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeleteNamespaceByNamespace,
                    Collections.singletonList(namespace));
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("delete from namespace where namespace = '%s' ", namespace);
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public List<String> listNamespaces() {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListNamespaces,
                    Collections.emptyList());
            if (jniWrapper == null) return null;
            List<Namespace> namespaceList = jniWrapper.getNamespaceList();
            return namespaceList.stream().map(Namespace::getNamespace).collect(Collectors.toList());
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select namespace from namespace";
        List<String> list = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String namespace = rs.getString("namespace");
                list.add(namespace);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public int updatePropertiesByNamespace(String namespace, String properties) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            return NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.UpdateNamespacePropertiesByNamespace,
                    Arrays.asList(namespace, properties));
        }
        int result = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;
        StringBuilder sb = new StringBuilder();
        sb.append("update namespace set ");
        sb.append(String.format("properties = '%s'", properties));
        sb.append(String.format(" where namespace = '%s'", namespace));
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sb.toString());
            result = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public static Namespace namespaceFromResultSet(ResultSet rs) throws SQLException {
        String comment = rs.getString("comment");
        return Namespace.newBuilder()
                .setNamespace(rs.getString("namespace"))
                .setProperties(rs.getString("properties"))
                .setComment(comment == null ? "" : comment)
                .setDomain(rs.getString("domain"))
                .build();
    }

    public static Namespace DEFAULT_NAMESPACE =
            Namespace.newBuilder()
                    .setNamespace(DBConfig.LAKESOUL_DEFAULT_NAMESPACE)
                    .setProperties("{}")
                    .setComment("")
                    .setDomain(AuthZContext.getInstance().getDomain())
                    .build();

}
