package com.ufpr.pedinte.core.dao;

import com.ufpr.pedinte.core.model.Cliente;
import com.ufpr.pedinte.core.model.ItemDoPedido;
import com.ufpr.pedinte.core.model.Pedido;
import com.ufpr.pedinte.core.model.Produto;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PedidoDAO {
    private Connection connection;

    public Pedido createPedido(Pedido pedido) throws SQLException {
        Pedido p = createPedido(pedido.getCliente().getId());
        if (pedido.getItens() != null) {
            for (ItemDoPedido each : pedido.getItens()) {
                saveItem(each, p.getId());
            }
        }
        return find(p.getId());
    }

    public Pedido createPedido(int clienteID) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        Pedido result = findPedidoByClient(clienteID, false);
        if (result.getData() != null) {
            return result;
        }
        String insert = "INSERT INTO pedido (data, cliente_fk) VALUES (NOW(), ?)";
        try {
            PreparedStatement ps = connection.prepareStatement(insert);
            ps.setInt(1, clienteID);
            ps.execute();
            Pedido p = findPedidoByClient(clienteID, true);
            return p;
        } catch (SQLException sqle) {
            throw new SQLException(sqle.getMessage());
        } finally {
            this.connection.close();
        }
    }

    public Pedido updatePedido(Pedido pedido) throws SQLException {
        removeItens(pedido.getId());
        for(ItemDoPedido each : pedido.getItens()) {
            saveItem(each, pedido.getId());
        }
        return find(pedido.getId());
    }

    public boolean deletePedido(Pedido pedido) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String remove = "DELETE FROM pedido " +
                "WHERE id = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(remove);
            ps.setInt(1, pedido.getId());
            ps.execute();
            return true;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            this.connection.close();
        }
        return false;
    }

    public List<Pedido> fetchAll() throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String findAll = "SELECT p.id as pid, p.data AS data, " +
                " c.id AS clienteId, c.nome as nomeCliente, c.sobrenome as sobrenome, c.cpf as cpf, " +
                "SUM(pp.quantidade) as quantidade " +
                "FROM pedido p " +
                "JOIN cliente c ON p.cliente_fk = c.id " +
                "JOIN produto_pedido pp on p.id = pp.pedido_fk " +
                "JOIN produto prd on pp.produto_fk = prd.id " +
                "GROUP BY p.id ORDER BY p.id DESC";
        try {
            PreparedStatement itensStatement = connection.prepareStatement(findAll);
            ResultSet rs = itensStatement.executeQuery();
            List<Pedido> pedidos = new ArrayList<>();

            while (rs.next()) {
                Cliente cliente = new Cliente();
                Pedido pedido = new Pedido();
                ItemDoPedido item = new ItemDoPedido();
                List<ItemDoPedido> itens = new ArrayList<>();

                cliente.setId(rs.getInt("clienteId"));
                cliente.setNome(rs.getString("nomeCliente"));
                cliente.setSobrenome(rs.getString("sobrenome"));
                cliente.setCpf(rs.getString("cpf"));

                item.setQuantidade(rs.getInt("quantidade"));

                pedido.setData(rs.getDate("data"));
                pedido.setId(rs.getInt("pid"));
                pedido.setItens(itens);
                pedido.setCliente(cliente);

                itens.add(item);
                pedidos.add(pedido);
            }
            return pedidos;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SQLException();
        } finally {
            this.connection.close();
        }
    }

    private void removeItens(int id) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String clearPedido = "DELETE FROM produto_pedido " +
                "WHERE pedido_fk = ?";
        try {
            PreparedStatement clearStatement = connection.prepareStatement(clearPedido);
            clearStatement.setInt(1, id);
            clearStatement.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            this.connection.close();
        }
    }

    private Pedido findPedidoById(int id) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        Pedido result = new Pedido();
        String find = "SELECT p.data, c.id, c.nome, c.sobrenome, c.cpf " +
                "FROM pedido p " +
                "JOIN cliente c ON p.cliente_fk = c.id " +
                "WHERE p.id = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(find);
            ps.setInt(1, id);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                result.setId(id);
                result.setData(rs.getString("data"));
                Cliente c = new Cliente();
                c.setId(rs.getInt("id"));
                c.setNome(rs.getString("nome"));
                c.setSobrenome(rs.getString("sobrenome"));
                c.setCpf(rs.getString("cpf"));
                result.setCliente(c);
            }
        }  catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.connection.close();
        }
        return result;
    }

    public Pedido find(int id) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        Pedido result = new Pedido();
        String findOne = "SELECT p.data AS data," +
                "c.id AS cid, c.nome, c.sobrenome, c.cpf," +
                "prd.id AS proid, prd.descricao AS prodesc," +
                "pp.quantidade " +
                "FROM pedido p " +
                "JOIN cliente c ON p.cliente_fk = c.id " +
                "JOIN produto_pedido pp on p.id = pp.pedido_fk " +
                "JOIN produto prd on pp.produto_fk = prd.id " +
                "WHERE p.id = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(findOne);
            ps.setInt(1, id);
            ResultSet rs = ps.executeQuery();
            Cliente c = new Cliente();
            Produto p = new Produto();
            ItemDoPedido each = new ItemDoPedido();
            List<ItemDoPedido> itens = new ArrayList<>();
            if (rs.next()) {
                c.setId(rs.getInt("cid"));
                c.setNome(rs.getString("nome"));
                c.setSobrenome(rs.getString("sobrenome"));
                c.setCpf(rs.getString("cpf"));

                p.setDescricao(rs.getString("prodesc"));
                p.setId(rs.getInt("proid"));

                each.setProduto(p);
                each.setQuantidade(rs.getInt(rs.getInt("quantidade")));
                itens.add(each);

                result.setData(rs.getDate("data"));
            }
            result.setId(id);
            result.setCliente(c);
            result.setItens(itens);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            this.connection.close();
        }
        return result;
    }

    private Pedido findPedidoByClient(int clienteID, boolean closeConnection) throws SQLException {
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

    public void saveItem(ItemDoPedido item, int pedido) throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
        String add = "INSERT INTO produto_pedido (produto_fk, pedido_fk, quantidade) VALUES (?, ?, ?)";
        try {
            PreparedStatement ps = connection.prepareStatement(add);
            ps.setInt(1, item.getProduto().getId());
            ps.setInt(2, pedido);
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
