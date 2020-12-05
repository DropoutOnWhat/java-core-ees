package com.ufpr.pedinte.core.dao;

import com.ufpr.pedinte.core.model.Cliente;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ClienteDAO {

    private Connection connection;

    public boolean createCliente(Cliente c) throws SQLException {
        boolean result = false;
        this.connection = new ConnectionFactory().getConnection();
        String insert = "INSERT INTO cliente (cpf, nome, sobrenome) VALUES (?,?,?)";
        try {
            PreparedStatement ps = connection.prepareStatement(insert);
            ps.setString(1, c.getCpf());
            ps.setString(2, c.getNome());
            ps.setString(3, c.getSobrenome());

            result = ps.execute();
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        } finally {
            this.connection.close();
        }
        return result;
    }

    public List<Cliente> fetchAll() throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String selectAll = "SELECT * FROM cliente";
        try {
            PreparedStatement ps = connection.prepareStatement(selectAll);
            ResultSet rs = ps.executeQuery();
            System.out.println(rs != null ? "Encontrei clientes." : "Não encontrei clientes.");

            List<Cliente> result = new ArrayList<>();
            while (rs.next()) {
                Cliente each = new Cliente();
                each.setId(rs.getInt("id"));
                each.setCpf(rs.getString("cpf"));
                each.setNome(rs.getString("nome"));
                each.setSobrenome(rs.getString("sobrenome"));
                result.add(each);
            }
            return result;
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        } finally {
            this.connection.close();
        }
    }

    public boolean updateCliente(Cliente c) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String update = "UPDATE cliente SET nome = ?, sobrenome = ?, cpf = ?  WHERE id = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(update);
            ps.setString(1, c.getNome());
            ps.setString(2, c.getSobrenome());
            ps.setString(3, c.getCpf());
            ps.setInt(4, c.getId());

            return ps.execute();
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        } finally {
            this.connection.close();
        }
    }

    public boolean removeCliente(Cliente c) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String delete = "DELETE FROM cliente WHERE id = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(delete);
            ps.setInt(1, c.getId());

            return ps.execute();
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        } finally {
            this.connection.close();
        }
    }

    public Cliente find(int id) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String select = "SELECT id, nome, sobrenome, cpf FROM cliente WHERE id = ?";
        Cliente result = new Cliente();
        try {
            PreparedStatement ps = connection.prepareStatement(select);
            ps.setInt(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                result.setId(rs.getInt("id"));
                result.setCpf(rs.getString("cpf"));
                result.setNome(rs.getString("nome"));
                result.setSobrenome(rs.getString("sobrenome"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            this.connection.close();
        }
        return result;
    }
}
