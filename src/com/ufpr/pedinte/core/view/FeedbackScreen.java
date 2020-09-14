package com.ufpr.pedinte.core.view;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.event.*;
import java.util.List;

public class FeedbackScreen extends JDialog {
    private JPanel contentPane;
    private JButton buttonOK;
    private JButton buttonCancel;
    private JTable errorTable;
    private JLabel feedbackLabel;

    public FeedbackScreen(List<String> feedback) {
        setTitle("# PEDINTE PEDIDOS --> Aviso #");
        setContentPane(contentPane);
        setModal(true);
        getRootPane().setDefaultButton(buttonOK);

        mostraFeedback(feedback);

        buttonOK.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                onOK();
            }
        });

        buttonCancel.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                onCancel();
            }
        });

        // call onCancel() when cross is clicked
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                onCancel();
            }
        });

        // call onCancel() on ESCAPE
        contentPane.registerKeyboardAction(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                onCancel();
            }
        }, KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT);
    }

    private void onOK() {
        // add your code here
        dispose();
    }

    private void mostraFeedback(List<String> feedback) {
        DefaultTableModel model = new DefaultTableModel();
        feedbackLabel.setText("Atenção!");
        model.addColumn("feedback", feedback.toArray());
        errorTable.setModel(model);
    }

    private void onCancel() {
        // add your code here if necessary
        dispose();
    }

    public static void main(List<String> erros) {
        FeedbackScreen dialog = new FeedbackScreen(erros);
        dialog.pack();
        dialog.setVisible(true);
//        System.exit(0);
    }
}
