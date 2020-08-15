package view;

import com.mysql.cj.util.StringUtils;
import dao.ProdutoDAO;
import model.Produto;
import model.ProdutoTableModel;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ProdutoScreen extends JFrame {
    private static final CharSequence FOREIGNKEY = "foreign key constraint fails";
    private JPanel mainPanel;
    private JPanel buttonPanel;
    private JButton pedidosButton;
    private JButton clientesButton;
    private JButton inicioButton;
    private JPanel formPanel;
    private JButton salvarButton;
    private JTextField descricaoField;
    private JButton removerButton;
    private JButton limparButton;
    private JTable produtoTable;
    private JLabel idField;

    public static void main(String[] args) {
        new ProdutoScreen();
    }

    public ProdutoScreen() {
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
        pedidosButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                PedidoScreen screen = new PedidoScreen();
                screen.setBounds(600,300, 600,550);
                screen.setVisible(true);
                dispose();
            }
        });
        salvarButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                Produto p = new Produto();
                p.setDescricao(descricaoField.getText());
                if(StringUtils.isNullOrEmpty(idField.getText())) {
                    salvarProduto(p);
                } else {
                    p.setId(Integer.valueOf(idField.getText()));
                    atualizarProduto(p);
                }
            }
        });
        removerButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                for (int each : produtoTable.getSelectedRows()) {
                    removeProduto(each);
                }
            }
        });
        produtoTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                super.mouseClicked(e);
                preencheFormulario(produtoTable.getSelectedRow());
            }
        });
        limparButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                limpaFormulario();
            }
        });
    }

    private void removeProduto(int row) {
        try {
            ProdutoDAO dao = new ProdutoDAO();
            String[] cols = {"ID", "Nome", "Sobrenome", "CPF"};
            ProdutoTableModel model = new ProdutoTableModel(dao.fetchAll(), cols);
            int id = Integer.valueOf(model.getValueAt(row, 0).toString());
            Produto toRemove = new Produto();
            toRemove.setId(id);
            dao.removeProduto(toRemove);
            popularTabela();
            limpaFormulario();
        } catch (SQLException sqle) {
            if(sqle.getMessage().contains(FOREIGNKEY)) {
                List<String> erros = new ArrayList<>();
                erros.add("Impossível remover Produto que está em um Pedido aberto!");
                FeedbackScreen screen = new FeedbackScreen(erros);
                screen.setBounds(700,350, 380,160);
                screen.setVisible(true);
            }
            sqle.printStackTrace();
        }
    }

    private void salvarProduto(Produto p) {
        try {
            System.out.println("Salvando produto.");
            ProdutoDAO dao = new ProdutoDAO();
            dao.createProduto(p);
            popularTabela();
            limpaFormulario();
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

    private void atualizarProduto(Produto p) {
        try {
            System.out.println("Atualizando cliente.");
            ProdutoDAO dao = new ProdutoDAO();
            dao.updateProduto(p);
            popularTabela();
            limpaFormulario();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void init() {
        setTitle("# PEDINTE PEDIDOS #");
        setBounds(600,300, 600,550);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setContentPane(mainPanel);
        mainPanel.setBorder(new EmptyBorder(10, 10, 10, 10));
        setVisible(true);
        popularTabela();
    }

    private void popularTabela() {
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

    private void preencheFormulario(int row) {
        try {
            ProdutoDAO dao = new ProdutoDAO();
            String[] cols = {"ID", "Nome", "Sobrenome", "CPF"};
            List<Produto> content = dao.fetchAll();
            ProdutoTableModel model = new ProdutoTableModel(content, cols);

            idField.setText(model.getValueAt(row, 0).toString());
            descricaoField.setText((String) model.getValueAt(row, 1));
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        } catch (ArrayIndexOutOfBoundsException aob) {

        }
    }

    private void limpaFormulario() {
        idField.setText("");
        descricaoField.setText("");
    }
}
