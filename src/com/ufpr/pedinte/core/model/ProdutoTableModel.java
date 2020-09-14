package com.ufpr.pedinte.core.model;

import javax.swing.table.AbstractTableModel;
import java.util.ArrayList;
import java.util.List;

public class ProdutoTableModel extends AbstractTableModel {
    List<Produto> content = new ArrayList<>();
    String[] cols = {};

    public ProdutoTableModel(List<Produto> content, String[] cols) {
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

    public void addRow(Produto p) {
        this.content.add(p);
        this.fireTableDataChanged();
    }

    public List<Produto> getContent() {
        return content;
    }

    public void setContent(List<Produto> content) {
        this.content = content;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        switch (columnIndex) {
            case 0:
                return content.get(rowIndex).getId();
            case 1:
                return content.get(rowIndex).getDescricao();
            default:
                return null;
        }
    }
}
