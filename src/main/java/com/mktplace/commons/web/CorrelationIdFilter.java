package com.mktplace.commons.web;

import com.mktplace.commons.events.EventContext;
import com.mktplace.commons.events.EventContextHolder;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.util.UuidV7;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Aceita ou gera X-Correlation-Id, ecoa na resposta, coloca no MDC e no EventContext da thread:
 * todo evento publicado durante a requisição herda esse correlationId (ADR 0003).
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(CorrelationIdFilter.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String incoming = request.getHeader(EventHeaders.HTTP_CORRELATION_ID);
        String correlationId = incoming == null || incoming.isBlank() ? UuidV7.generate().toString() : incoming.trim();
        response.setHeader(EventHeaders.HTTP_CORRELATION_ID, correlationId);
        EventContextHolder.set(new EventContext(correlationId, null));
        long start = System.nanoTime();
        try {
            chain.doFilter(request, response);
        } finally {
            long ms = (System.nanoTime() - start) / 1_000_000;
            String line = "http {} {} -> {} ({} ms)";
            if (request.getRequestURI().startsWith("/actuator")) {
                log.debug(line, request.getMethod(), request.getRequestURI(), response.getStatus(), ms);
            } else {
                log.info(line, request.getMethod(), request.getRequestURI(), response.getStatus(), ms);
            }
            EventContextHolder.clear();
        }
    }
}
