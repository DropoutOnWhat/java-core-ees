package com.ufpr.pedinte.core.model;


import java.util.Date;
import java.util.List;

public class Pedido {

    private int id;
    private Date data;
    private Cliente cliente;
    private List<ItemDoPedido> itens;

    public Pedido() {
    }

    public Pedido(int id, Date data, Cliente cliente, List<ItemDoPedido> itens) {
        this.id = id;
        this.data = data;
        this.cliente = cliente;
        this.itens = itens;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Date getData() {
        return data;
    }

    public void setData(Date data) {
        this.data = data;
    }

    public Cliente getCliente() {
        return cliente;
    }

    public void setCliente(Cliente cliente) {
        this.cliente = cliente;
    }

    public List<ItemDoPedido> getItens() {
        return itens;
    }

    public void setItens(List<ItemDoPedido> itens) {
        this.itens = itens;
    }

    public void setData(String data) {
    }
}
