package com.ufpr.pedinte.core.model;

import javax.swing.table.AbstractTableModel;
import java.util.ArrayList;
import java.util.List;

public class ClienteTableModel extends AbstractTableModel {
    List<Cliente> content = new ArrayList<>();
    String[] cols = {};

    public ClienteTableModel(List<Cliente> content, String[] cols) {
        this.content = content;
        this.cols = cols;
    }

    @Override
    public int getRowCount() {
        return content.size();
    }

    @Override
    public int getColumnCount() {
        return cols.length;
    }

    public void addRow(Cliente c) {
        this.content.add(c);
        this.fireTableDataChanged();
    }

    public List<Cliente> getContent() {
        return content;
    }

    public void setContent(List<Cliente> content) {
        this.content = content;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        switch (columnIndex) {
            case 0:
                return content.get(rowIndex).getId();
            case 1:
                return content.get(rowIndex).getNome();
            case 2:
                return content.get(rowIndex).getSobrenome();
            case 3:
                return content.get(rowIndex).getCpf();
            default:
                return null;
        }
    }
}
