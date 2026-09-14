package com.mktplace.order;

import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.util.UuidV7;
import com.mktplace.commons.web.ConflictException;
import com.mktplace.commons.web.NotFoundException;
import jakarta.validation.Valid;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
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
import java.util.UUID;

/** Clientes, mínimo da fatia vertical (criar e detalhar); a largura entra na fase 6. */
@RestController
@RequestMapping("/api/v1/customers")
@Profile("order")
public class CustomerController {

    private final CustomerRepository customers;
    private final MktplaceProperties props;
    private final Clock clock;

    public CustomerController(CustomerRepository customers, MktplaceProperties props, Clock clock) {
        this.customers = customers;
        this.props = props;
        this.clock = clock;
    }

    @PostMapping
    @Transactional
    public ResponseEntity<OrderDtos.CustomerResponse> create(@Valid @RequestBody OrderDtos.CustomerRequest request) {
        if (customers.findByEmailIgnoreCase(request.email()).isPresent()) {
            throw new ConflictException("email-already-exists", "Já existe cliente com o email " + request.email());
        }
        var customer = customers.save(new Customer(UuidV7.generate(), request.name(), request.email(), OffsetDateTime.now(clock)));
        var response = OrderDtos.CustomerResponse.from(customer, props);
        return ResponseEntity.created(URI.create("/api/v1/customers/" + customer.getId()))
                .eTag("\"" + customer.getVersion() + "\"").body(response);
    }

    @GetMapping("/{customerId}")
    public ResponseEntity<OrderDtos.CustomerResponse> get(@PathVariable UUID customerId) {
        var customer = customers.findById(customerId)
                .orElseThrow(() -> new NotFoundException("customer-not-found", "Cliente não encontrado: " + customerId));
        return ResponseEntity.ok().eTag("\"" + customer.getVersion() + "\"").body(OrderDtos.CustomerResponse.from(customer, props));
    }
}
