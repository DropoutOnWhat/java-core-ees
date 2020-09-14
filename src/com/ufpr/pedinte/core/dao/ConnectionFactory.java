package com.ufpr.pedinte.core.dao;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    public Connection getConnection() throws SQLException {
        try {
            String timezone = "useTimezone=true&serverTimezone=America/Sao_Paulo";
            return DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/pedinte_db?" + timezone,
                    "root",
                    "Cymbaline");
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        }
    }
}
