package com.ufpr.pedinte.core.model;

public class ItemDoPedido {

    int quantidade;
    Produto produto;

    public ItemDoPedido() {

    }

    public ItemDoPedido(int quantidade, Produto produto) {
        this.quantidade = quantidade;
        this.produto = produto;
    }

    public int getQuantidade() {
        return quantidade;
    }

    public void setQuantidade(int quantidade) {
        this.quantidade = quantidade;
    }

    public Produto getProduto() {
        return produto;
    }

    public void setProduto(Produto produto) {
        this.produto = produto;
    }
}
