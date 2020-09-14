package com.ufpr.pedinte.core.dao;

import com.ufpr.pedinte.core.model.ItemDoPedido;
import com.ufpr.pedinte.core.model.Pedido;
import com.ufpr.pedinte.core.model.Produto;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PedidoDAO {
    private Connection connection;

    public Pedido createPedido(int clienteID) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        Pedido result = findPedido(clienteID, false);
        if (result.getData() != null) {
            return result;
        }
        String insert = "INSERT INTO pedido (data, cliente_fk) VALUES (NOW(), ?)";
        try {
            PreparedStatement ps = connection.prepareStatement(insert);
            ps.setInt(1, clienteID);
            ps.execute();
            Pedido p = findPedido(clienteID, true);
            return p;
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        }
    }

    private Pedido findPedido(int clienteID, boolean closeConnection) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        Pedido result = new Pedido();
        String find = "SELECT id, data FROM pedido WHERE cliente_fk = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(find);
            ps.setInt(1, clienteID);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                result.setId(rs.getInt("id"));
                result.setData(rs.getString("data"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (closeConnection) {
                this.connection.close();
            }
        }
        return result;
    }

    public List<ItemDoPedido> findItensDoCliente(int clientID) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        List<ItemDoPedido> resultList = new ArrayList<>();
        String findItens = "SELECT prd.id AS proid, prd.descricao AS prodesc, pp.quantidade as quantidade " +
                "FROM produto_pedido pp " +
                "JOIN produto prd ON prd.id = pp.produto_fk " +
                "JOIN pedido pdd ON pp.pedido_fk = pdd.id " +
                "WHERE pdd.cliente_fk = ?";
        try {
            PreparedStatement itensStatement = connection.prepareStatement(findItens);
            itensStatement.setInt(1, clientID);
            ResultSet rs = itensStatement.executeQuery();
            while (rs.next()) {
                Produto produto = new Produto();
                produto.setId(rs.getInt("proid"));
                produto.setDescricao(rs.getString("prodesc"));
                ItemDoPedido item = new ItemDoPedido();
                item.setQuantidade(Integer.valueOf(rs.getString("quantidade")));
                item.setProduto(produto);
                resultList.add(item);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }

    public void saveItem(ItemDoPedido item, int comanda) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String add = "INSERT INTO produto_pedido (produto_fk, pedido_fk, quantidade) VALUES (?, ?, ?)";
        try {
            PreparedStatement ps = connection.prepareStatement(add);
            ps.setInt(1, item.getProduto().getId());
            ps.setInt(2, comanda);
            ps.setInt(3, item.getQuantidade());

            ps.execute();
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        } finally {
            this.connection.close();
        }
    }

    public void atualizaQuantidade(ItemDoPedido item, int cliente) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();

        String delete = "UPDATE produto_pedido pp " +
                "JOIN produto pdt ON pp.produto_fk = pdt.id " +
                "JOIN pedido pdd ON pp.pedido_fk = pdd.id " +
                "SET pp.quantidade = ? " +
                "WHERE pdt.descricao = ? " +
                "AND pdd.cliente_fk = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(delete);
            ps.setInt(1, item.getQuantidade());
            ps.setString(2, item.getProduto().getDescricao());
            ps.setInt(3, cliente);

            ps.execute();
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        } finally {
            this.connection.close();
        }
    }

    public void deleteGarbage(int cliente) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String delete = "DELETE pp FROM produto_pedido pp " +
                "JOIN pedido pdd ON pp.pedido_fk = pdd.id " +
                "WHERE 0 >= pp.quantidade " +
                "AND pdd.cliente_fk = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(delete);
            ps.setInt(1, cliente);

            ps.execute();
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        } finally {
            this.connection.close();
        }
    }
}
