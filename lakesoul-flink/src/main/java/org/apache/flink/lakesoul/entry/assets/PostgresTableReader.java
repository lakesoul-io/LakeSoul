package org.apache.flink.lakesoul.entry.assets;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class PostgresTableReader {

    // 数据库连接信息
    private String dbUrl;
    private String dbUser;
    private String dbPassword;

    public PostgresTableReader(String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    /**
     * 根据 table_id 获取表信息
     * @param tableId 要查询的表的主键ID
     * @return 包含 table_name, table_namespace, domain, creator 的 Map
     * @throws SQLException 如果数据库操作出错
     */
    public Map<String, String> getTableInfoById(String tableId) throws SQLException {
        // 定义要查询的字段
        final String[] fields = {"table_name", "table_namespace", "domain", "creator"};

        // 准备返回结果
        Map<String, String> result = new HashMap<>();

        String sql = "SELECT table_name, table_namespace, domain, creator " +
                "FROM table_info WHERE table_id = ?";

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement statement = connection.prepareStatement(sql)) {

            // 设置参数
            statement.setString(1, tableId);

            // 执行查询
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    for (String field : fields) {
                        result.put(field, rs.getString(field));
                    }
                }
            }
        }

        return result;
    }

    // 使用示例
    public static void main(String[] args) {
        // 配置数据库连接信息
        String dbUrl = "jdbc:postgresql://localhost:5432/lakesoul_test";
        String dbUser = "lakesoul_test";
        String dbPassword = "lakesoul_test";

        PostgresTableReader reader = new PostgresTableReader(dbUrl, dbUser, dbPassword);

        try {
            // 查询 table_id 为 1 的记录
            Map<String, String> tableInfo = reader.getTableInfoById("table_8f2dda2d-b2cc-40b0-97b0-65ee0fa9ef8b");

            if (tableInfo.isEmpty()) {
                System.out.println("未找到对应记录");
            } else {
                System.out.println("查询结果:");
                for (Map.Entry<String, String> entry : tableInfo.entrySet()) {
                    System.out.println(entry.getKey() + ": " + entry.getValue());
                }
            }
        } catch (SQLException e) {
            System.err.println("数据库操作出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
