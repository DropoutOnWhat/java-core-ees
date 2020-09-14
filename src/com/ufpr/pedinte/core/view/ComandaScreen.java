package com.ufpr.pedinte.core.view;


import com.ufpr.pedinte.core.dao.PedidoDAO;
import com.ufpr.pedinte.core.model.Comanda;
import com.ufpr.pedinte.core.model.ItemDoPedido;
import com.ufpr.pedinte.core.model.ItemTableModel;
import com.ufpr.pedinte.core.model.Produto;
import org.apache.commons.lang3.StringUtils;


import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ComandaScreen extends JFrame {
    private static final CharSequence FOREIGNKEY = "foreign key constraint fails";
    private JPanel buttonPanel;
    private JButton OKButton;
    private JButton removerButton;
    private JTable itemTable;
    private JPanel mainPanel;
    private JTextField quantiaField;

    private int cliID;

    public ComandaScreen(int clienteID) {
        this.cliID = clienteID;
        init();
        OKButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                dispose();
            }
        });
        removerButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (validaForm()) {
                    removeItem(itemTable.getSelectedRow());
                }
            }
        });
    }

    private boolean validaForm() {
        List<String> erros = new ArrayList<>();
        if (0 > itemTable.getSelectedRow()) {
            erros.add("Selecione um produto!");
        }
        if (StringUtils.isEmpty(quantiaField.getText())) {
            erros.add("Informe a quantia a ser removida!");
        } else if (0 >= Integer.valueOf(quantiaField.getText())) {
            erros.add("Quantidade inválida: deve ser maior que Zero.");
        }
        if (!erros.isEmpty()) {
            FeedbackScreen screen = new FeedbackScreen(erros);
            screen.setBounds(700, 350, 380, 160);
            screen.setVisible(true);
        }
        return erros.isEmpty();
    }

    private void init() {
        setTitle("# PEDINTE PEDIDOS --> Comanda #");
        setBounds(600, 300, 600, 550);
        setContentPane(mainPanel);
        mainPanel.setBorder(new EmptyBorder(10, 10, 10, 10));
        popularTabela();
        setVisible(true);
    }

    private void popularTabela() {
        try {
            PedidoDAO dao = new PedidoDAO();
            String[] cols = {"Produto", "Quantia"};
            List<ItemDoPedido> content = dao.findItensDoCliente(cliID);
            List<Comanda> itens = extractItens(content);
            ItemTableModel model = new ItemTableModel(itens, cols);
            itemTable.setModel(model);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

    private void removeItem(int row) {
        try {
            PedidoDAO dao = new PedidoDAO();
            String[] cols = {"Produto", "Descrição"};
            int atual = Integer.valueOf(itemTable.getValueAt(row, 1).toString());
            int q = Integer.valueOf(quantiaField.getText());
            ItemDoPedido toRemove = new ItemDoPedido();
            toRemove.setQuantidade(atual - q);
            Produto p = new Produto();
            p.setDescricao(String.valueOf(itemTable.getValueAt(row, 0)));
            toRemove.setProduto(p);

            dao.atualizaQuantidade(toRemove, cliID);
            popularTabela();
            quantiaField.setText("");
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

    private List<Comanda> extractItens(List<ItemDoPedido> content) throws SQLException {
        List<Comanda> itens = new ArrayList<>();
        for (ItemDoPedido each : content) {
            if (each.getQuantidade() > 0) {
                Comanda c = new Comanda();
                c.setProduto(each.getProduto().getDescricao());
                c.setQuantia(each.getQuantidade());
                itens.add(c);
            } else {
                PedidoDAO dao = new PedidoDAO();
                dao.deleteGarbage(cliID);
            }
        }
        return itens;
    }
}
