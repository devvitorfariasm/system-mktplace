package com.mktplace.inventory;

import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.util.UuidV7;
import com.mktplace.commons.web.ConflictException;
import com.mktplace.commons.web.NotFoundException;
import jakarta.validation.Valid;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Clock;
import java.time.OffsetDateTime;

/** Produtos, mínimo da fatia vertical: cadastrar (com saldo inicial) e detalhar. */
@RestController
@RequestMapping("/api/v1/products")
@Profile("inventory")
public class ProductController {

    private final ProductRepository products;
    private final JdbcClient jdbc;
    private final MktplaceProperties props;
    private final Clock clock;

    public ProductController(ProductRepository products, JdbcClient jdbc, MktplaceProperties props, Clock clock) {
        this.products = products;
        this.jdbc = jdbc;
        this.props = props;
        this.clock = clock;
    }

    @PostMapping
    @Transactional
    public ResponseEntity<ProductDtos.Response> create(@Valid @RequestBody ProductDtos.CreateRequest request) {
        if (products.existsById(request.sku())) {
            throw new ConflictException("sku-already-exists", "Já existe produto com sku " + request.sku());
        }
        var now = OffsetDateTime.now(clock);
        var product = products.saveAndFlush(new Product(request.sku(), request.name(), request.price(), request.initialStock(), now));
        // todo saldo nasce de uma movimentação: o histórico nunca mente
        jdbc.sql("""
                INSERT INTO stock_movements (id, sku, delta, kind, reason, balance_total, balance_reserved)
                VALUES (:id, :sku, :delta, 'INITIAL', 'Saldo inicial', :total, 0)
                """)
                .param("id", UuidV7.generate()).param("sku", product.getSku()).param("delta", request.initialStock())
                .param("total", request.initialStock()).update();
        return ResponseEntity.created(URI.create("/api/v1/products/" + product.getSku()))
                .eTag("\"" + product.getVersion() + "\"").body(get(product.getSku()).getBody());
    }

    @GetMapping("/{sku}")
    public ResponseEntity<ProductDtos.Response> get(@PathVariable String sku) {
        var response = jdbc.sql("""
                SELECT sku, name, price, total - reserved AS available, reserved, total, active, version, created_at
                FROM products WHERE sku = :sku
                """)
                .param("sku", sku)
                .query((rs, i) -> new ProductDtos.Response(rs.getString("sku"), rs.getString("name"), rs.getLong("price"),
                        rs.getInt("available"), rs.getInt("reserved"), rs.getInt("total"), rs.getBoolean("active"),
                        rs.getLong("version"), rs.getObject("created_at", OffsetDateTime.class).atZoneSameInstant(props.zone()).toOffsetDateTime()))
                .optional()
                .orElseThrow(() -> new NotFoundException("product-not-found", "Produto não encontrado: " + sku));
        return ResponseEntity.ok().eTag("\"" + response.version() + "\"").body(response);
    }
}
