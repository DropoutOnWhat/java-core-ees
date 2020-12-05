package com.ufpr.pedinte.core.dao;

import com.ufpr.pedinte.core.model.Produto;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ProdutoDAO {
    private Connection connection;

    public boolean createProduto(Produto p) throws SQLException {
        boolean result = false;
        this.connection = new ConnectionFactory().getConnection();
        String insert = "INSERT INTO produto (descricao) VALUES (?)";
        try {
            PreparedStatement ps = connection.prepareStatement(insert);
            ps.setString(1, p.getDescricao());

            result = ps.execute();
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        } finally {
            this.connection.close();
        }
        return result;
    }

    public List<Produto> fetchAll() throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String selectAll = "SELECT * FROM produto";
        PreparedStatement ps = connection.prepareStatement(selectAll);
        ResultSet rs = ps.executeQuery();

        List<Produto> result = new ArrayList<>();
        while (rs.next()) {
            Produto each = new Produto();
            each.setId(rs.getInt("id"));
            each.setDescricao(rs.getString("descricao"));
            result.add(each);
        }
        return result;
    }

    public boolean updateProduto(Produto p) throws SQLException {
        boolean result = false;
        this.connection = new ConnectionFactory().getConnection();
        String update = "UPDATE produto SET descricao = ? WHERE id = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(update);
            ps.setString(1, p.getDescricao());
            ps.setInt(2, p.getId());

            result = ps.execute();
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        } finally {
            this.connection.close();
        }
        return result;
    }

    public boolean removeProduto(Produto p) throws SQLException {
        boolean result = false;
        this.connection = new ConnectionFactory().getConnection();
        String delete = "DELETE FROM produto WHERE id = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(delete);
            ps.setInt(1, p.getId());

            result = ps.execute();
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        } finally {
            this.connection.close();
        }
        return result;
    }

    public Produto find(int id) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String select = "SELECT id, descricao FROM produto WHERE id = ?";
        Produto result = new Produto();
        try {
            PreparedStatement ps = connection.prepareStatement(select);
            ps.setInt(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                result.setId(rs.getInt("id"));
                result.setDescricao(rs.getString("descricao"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            this.connection.close();
        }
        return result;
    }
}
