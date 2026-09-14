package com.mktplace.inventory;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.time.OffsetDateTime;

public final class ProductDtos {

    private ProductDtos() {
    }

    public record CreateRequest(
            @NotBlank(message = "sku é obrigatório") String sku,
            @NotBlank(message = "Nome é obrigatório") String name,
            @Min(value = 0, message = "Preço não pode ser negativo") long price,
            @Min(value = 0, message = "Estoque inicial não pode ser negativo") int initialStock) {
    }

    /** available = total - reserved, calculado pelo banco na consulta. */
    public record Response(String sku, String name, long price, int available, int reserved, int total, boolean active,
                           long version, OffsetDateTime createdAt) {
    }
}
