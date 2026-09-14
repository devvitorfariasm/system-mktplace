package com.mktplace.inventory;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.OffsetDateTime;

/**
 * Produto com saldo total/reservado. A reserva NÃO passa por aqui: é um UPDATE atômico condicional
 * (ReservationService) para evitar overselling sob concorrência; a entidade serve ao cadastro e leitura.
 */
@Entity
@Table(name = "products")
public class Product {

    @Id
    private String sku;
    @Column(nullable = false)
    private String name;
    @Column(nullable = false)
    private long price;
    @Column(nullable = false)
    private int total;
    @Column(nullable = false)
    private int reserved;
    @Column(nullable = false)
    private boolean active = true;
    @Version
    private long version;
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected Product() {
    }

    public Product(String sku, String name, long price, int initialStock, OffsetDateTime now) {
        this.sku = sku;
        this.name = name;
        this.price = price;
        this.total = initialStock;
        this.reserved = 0;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public String getSku() {
        return sku;
    }

    public String getName() {
        return name;
    }

    public long getPrice() {
        return price;
    }

    public int getTotal() {
        return total;
    }

    public int getReserved() {
        return reserved;
    }

    public boolean isActive() {
        return active;
    }

    public long getVersion() {
        return version;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}
