package com.ufpr.pedinte.core.dao;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    public Connection getConnection() throws SQLException {
        try {
            String timezone = "useTimezone=true&serverTimezone=America/Sao_Paulo";
            return DriverManager.getConnection(
                    "jdbc:mysql://35.198.31.4:3306/pedinte?",
                    "Amsterdam",
                    ".Netherlands#");
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        }
    }
}
