package com.ufpr.pedinte.core.view;

import com.ufpr.pedinte.core.dao.ClienteDAO;
import com.ufpr.pedinte.core.model.Cliente;
import com.ufpr.pedinte.core.model.ClienteTableModel;
import org.apache.commons.lang3.StringUtils;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.event.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ClienteScreen extends JFrame {

    private static final CharSequence FOREIGNKEY = "foreign key constraint fails";
    private static final CharSequence DUPLICATEKEY = "Duplicate entry";
    private JPanel mainPanel;
    private JPanel formPanel;
    private JPanel buttonPanel;
    private JButton salvarButton;
    private JTextField nomeField;
    private JTextField sobrenomeField;
    private JTextField cpfField;
    private JButton pedidosButton;
    private JButton produtosButton;
    private JButton inicioButton;
    private JTable clientTable;
    private JButton removerButton;
    private JLabel idField;
    private JButton limparButton;

    public static void main(String[] args) {
        new ClienteScreen();
    }

    public ClienteScreen() {
        init();
        inicioButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                BemVindo screen = new BemVindo();
                screen.setBounds(600, 300, 600, 550);
                screen.setVisible(true);
                dispose();
            }
        });
        produtosButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ProdutoScreen screen = new ProdutoScreen();
                screen.setBounds(600, 300, 600, 550);
                screen.setVisible(true);
                dispose();
            }
        });
        pedidosButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                PedidoScreen screen = new PedidoScreen();
                screen.setBounds(600, 300, 600, 550);
                screen.setVisible(true);
                dispose();
            }
        });
        salvarButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                Cliente c = new Cliente();
                c.setNome(nomeField.getText());
                c.setSobrenome(sobrenomeField.getText());
                c.setCpf(cpfField.getText());
                if (StringUtils.isEmpty(idField.getText())) {
                    if (validaForm()) {
                        salvarCliente(c);
                    }
                } else {
                    c.setId(Integer.valueOf(idField.getText()));
                    atualizarCliente(c);
                }
            }
        });
        removerButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                for (int each : clientTable.getSelectedRows()) {
                    removeCliente(each);
                }
            }
        });
        clientTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                super.mouseClicked(e);
                preencheFormulario(clientTable.getSelectedRow());
            }
        });
        limparButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                limpaFormulario();
            }
        });
    }

    private boolean validaForm() {
        List<String> erros = new ArrayList<>();
        if (StringUtils.isEmpty(nomeField.getText())) {
            erros.add("Informe o nome do cliente!");
        }
        if (StringUtils.isEmpty(sobrenomeField.getText())) {
            erros.add("Informe o sobrenome do cliente!");
        }
        if (StringUtils.isEmpty(cpfField.getText())) {
            erros.add("Informe o CPF do cliente!");
        } else if (!cpfField.getText().matches("[0-9]*")) {
            erros.add("CPF deve conter apenas números!");
        } else if (cpfField.getText().length() != 11) {
            erros.add("CPF deve conter 11 dígitos!");
        }
        if (!erros.isEmpty()) {
            FeedbackScreen screen = new FeedbackScreen(erros);
            screen.setBounds(700, 350, 380, 180);
            screen.setVisible(true);
        }
        return erros.isEmpty();
    }

    private void limpaFormulario() {
        idField.setText("");
        nomeField.setText("");
        sobrenomeField.setText("");
        cpfField.setText("");
    }

    private void preencheFormulario(int row) {
        try {
            ClienteDAO dao = new ClienteDAO();
            String[] cols = {"ID", "Nome", "Sobrenome", "CPF"};
            List<Cliente> content = dao.fetchAll();
            ClienteTableModel model = new ClienteTableModel(content, cols);

            idField.setText(model.getValueAt(row, 0).toString());
            nomeField.setText((String) model.getValueAt(row, 1));
            sobrenomeField.setText((String) model.getValueAt(row, 2));
            cpfField.setText((String) model.getValueAt(row, 3));
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        } catch (ArrayIndexOutOfBoundsException aob) {

        }
    }

    private void removeCliente(int row) {
        try {
            ClienteDAO dao = new ClienteDAO();
            String[] cols = {"ID", "Nome", "Sobrenome", "CPF"};
            ClienteTableModel model = new ClienteTableModel(dao.fetchAll(), cols);
            int id = Integer.valueOf(model.getValueAt(row, 0).toString());
            Cliente toRemove = new Cliente();
            toRemove.setId(id);
            dao.removeCliente(toRemove);
            popularTabela();
            limpaFormulario();
        } catch (SQLException sqle) {
            if (sqle.getMessage().contains(FOREIGNKEY)) {
                List<String> erros = new ArrayList<>();
                erros.add("Impossível remover Cliente com Pedido aberto!");
                FeedbackScreen screen = new FeedbackScreen(erros);
                screen.setBounds(700, 350, 380, 160);
                screen.setVisible(true);
            }
            sqle.printStackTrace();
        }
    }

    private void salvarCliente(Cliente c) {
        try {
            System.out.println("Salvando cliente.");
            ClienteDAO dao = new ClienteDAO();
            dao.createCliente(c);
            popularTabela();
            limpaFormulario();
        } catch (SQLException sqle) {
            if (sqle.getMessage().contains(DUPLICATEKEY)) {
                List<String> erros = new ArrayList<>();
                erros.add("Um cliente com este CPF já existe!");
                FeedbackScreen screen = new FeedbackScreen(erros);
                screen.setBounds(700, 350, 380, 160);
                screen.setVisible(true);
            }
            sqle.printStackTrace();
        }
    }

    private void atualizarCliente(Cliente c) {
        try {
            System.out.println("Atualizando cliente.");
            ClienteDAO dao = new ClienteDAO();
            dao.updateCliente(c);
            popularTabela();
            limpaFormulario();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void popularTabela() {
        try {
            ClienteDAO dao = new ClienteDAO();
            String[] cols = {"ID", "Nome", "Sobrenome", "CPF"};
            List<Cliente> content = dao.fetchAll();
            ClienteTableModel model = new ClienteTableModel(content, cols);
            clientTable.setModel(model);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

    private void init() {
        setTitle("# PEDINTE PEDIDOS --> Gerenciar Clientes #");
        setBounds(600, 300, 600, 550);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setContentPane(mainPanel);
        mainPanel.setBorder(new EmptyBorder(10, 10, 10, 10));
        setVisible(true);
        popularTabela();
    }
}
