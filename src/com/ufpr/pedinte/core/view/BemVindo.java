package com.ufpr.pedinte.core.view;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class BemVindo extends JFrame {
    private JPanel mainPanel;
    private JLabel texto;
    private JButton clientesButton;
    private JButton pedidosButton;
    private JButton produtosButton;
    private JPanel buttonPanel;
    private JLabel text01;
    private JLabel text02;


    public static void main(String[] args) {
        new BemVindo();
    }

    public BemVindo() {
        init();
        clientesButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ClienteScreen screen = new ClienteScreen();
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
    }

    private void init() {
        setTitle("# PEDINTE PEDIDOS --> Bem Vindo! #");
        setBounds(600, 300, 600, 550);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setContentPane(mainPanel);
        mainPanel.setBorder(new EmptyBorder(10, 10, 10, 10));
        setVisible(true);
    }
}
