package com.ufpr.pedinte.core.model;

import java.util.List;

public class Pedido {

    private int id;
    private String data;
    private Cliente cliente;
    private List<ItemDoPedido> itens;

    public Pedido() {

    }

    public Pedido(int id, String data, Cliente cliente, List<ItemDoPedido> itens) {
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

    public String getData() {
        return data;
    }

    public void setData(String data) {
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
}
