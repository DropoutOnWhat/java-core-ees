package com.ufpr.pedinte.core.model;

import javax.swing.table.AbstractTableModel;
import java.util.ArrayList;
import java.util.List;

public class ItemTableModel extends AbstractTableModel {
    List<Comanda> content = new ArrayList<>();
    String[] cols = {};

    public ItemTableModel(List<Comanda> content, String[] cols) {
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

    public void addRow(Comanda c) {
        this.content.add(c);
        this.fireTableDataChanged();
    }

    public List<Comanda> getContent() {
        return content;
    }

    public void setContent(List<Comanda> content) {
        this.content = content;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        switch (columnIndex) {
            case 0:
                return content.get(rowIndex).getProduto();
            case 1:
                return content.get(rowIndex).getQuantia();
            default:
                return null;
        }
    }
}
