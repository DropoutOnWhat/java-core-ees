package view;

import com.mysql.cj.util.StringUtils;
import dao.ClienteDAO;
import dao.PedidoDAO;
import dao.ProdutoDAO;
import model.*;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.SQLException;
import java.util.*;

public class PedidoScreen extends JFrame {
    private JPanel buttonPanel;
    private JButton produtosButton;
    private JButton clientesButton;
    private JButton inicioButton;
    private JPanel mainPanel;
    private JButton detalhesButton;
    private JTable clienteTable;
    private JButton adicionarButton;
    private JButton removerButton;
    private JTextField quantiaField;
    private JTable produtoTable;

    public static void main(String[] args) {
        new PedidoScreen();
    }

    public PedidoScreen() {
        init();
        inicioButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                BemVindo screen = new BemVindo();
                screen.setBounds(600,300, 600,550);
                screen.setVisible(true);
                dispose();
            }
        });
        clientesButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ClienteScreen screen = new ClienteScreen();
                screen.setBounds(600,300, 600,550);
                screen.setVisible(true);
                dispose();
            }
        });
        produtosButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ProdutoScreen screen = new ProdutoScreen();
                screen.setBounds(600,300, 600,550);
                screen.setVisible(true);
                dispose();
            }
        });
        detalhesButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                List<String> erros = new ArrayList<>();
                if(0 > clienteTable.getSelectedRow()) {
                    erros.add("Selecione um cliente!");
                }
                if(!erros.isEmpty()) {
                    FeedbackScreen screen = new FeedbackScreen(erros);
                    screen.setBounds(700,350, 380,160);
                    screen.setVisible(true);
                } else {
                    int clientID = Integer.valueOf(clienteTable.getValueAt(clienteTable.getSelectedRow(), 0).toString());
                    ComandaScreen screen = new ComandaScreen(clientID);
                    screen.setBounds(600, 300, 500, 450);
                    screen.setVisible(true);
                }
            }
        });
        adicionarButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if(validaForm()) {
                    int cliente = Integer.valueOf(clienteTable.getValueAt(clienteTable.getSelectedRow(), 0).toString());
                    int produto = Integer.valueOf(produtoTable.getValueAt(produtoTable.getSelectedRow(), 0).toString());
                    int quantia = Integer.valueOf(quantiaField.getText());
                    adicionarItem(produto, cliente, quantia);
                }
            }
        });
    }

    private boolean validaForm() {
        List<String> erros = new ArrayList<>();
        if(0 > clienteTable.getSelectedRow()) {
            erros.add("Selecione um cliente!");
        }
        if(0 > produtoTable.getSelectedRow()) {
            erros.add("Selecione um produto!");
        }
        if(StringUtils.isNullOrEmpty(quantiaField.getText())) {
            erros.add("Informe a quantia a ser adicionada!");
        }
        if(!erros.isEmpty()) {
            FeedbackScreen screen = new FeedbackScreen(erros);
            screen.setBounds(700,350, 380,160);
            screen.setVisible(true);
        }
        return erros.isEmpty();
    }


    private void adicionarItem(int p, int c, int q) {
        PedidoDAO dao = new PedidoDAO();
        ClienteDAO cdao = new ClienteDAO();
        ProdutoDAO pdao = new ProdutoDAO();
        boolean encontrou = false;
        try {
            Cliente cliente = cdao.find(c);
            Pedido comanda = dao.createPedido(cliente.getId());
            comanda.setCliente(cliente);
            comanda.setItens(dao.findItensDoCliente(cliente.getId()));
            ItemDoPedido item = new ItemDoPedido();
            item.setProduto(pdao.find(p));
            item.setQuantidade(q);

            for(ItemDoPedido each : comanda.getItens()) {
                if (each.getProduto().getId() == p) {
                    int atual = each.getQuantidade();
                    each.setQuantidade(atual + q);
                    dao.atualizaQuantidade(each, comanda.getId());
                    encontrou = true;
                }
            }
            if(!encontrou) {
                comanda.getItens().add(item);
                dao.saveItem(item, comanda.getId());
            }
            quantiaField.setText("");
            List<String> message = new ArrayList<>();
            message.add("Item adicionado com sucesso!");
            FeedbackScreen screen = new FeedbackScreen(message);
            screen.setTitle("Sucesso!");
            screen.setBounds(700,350, 380,160);
            screen.setVisible(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void init() {
        setTitle("# PEDINTE PEDIDOS --> Gerenciar Pedidos #");
        setBounds(600,300, 600,550);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setContentPane(mainPanel);
        mainPanel.setBorder(new EmptyBorder(10, 10, 10, 10));
        setVisible(true);
        popularTabelaCliente();
        popularTabelaProduto();
    }

    private void popularTabelaCliente() {
        try {
            ClienteDAO dao = new ClienteDAO();
            String[] cols = {"ID", "Nome", "Sobrenome", "CPF"};
            List<Cliente> content = dao.fetchAll();
            ClienteTableModel model = new ClienteTableModel(content, cols);
            clienteTable.setModel(model);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

    private void popularTabelaProduto() {
        try {
            ProdutoDAO dao = new ProdutoDAO();
            String[] cols = {"ID", "Descricao"};
            List<Produto> content = dao.fetchAll();
            ProdutoTableModel model = new ProdutoTableModel(content, cols);
            produtoTable.setModel(model);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

}
