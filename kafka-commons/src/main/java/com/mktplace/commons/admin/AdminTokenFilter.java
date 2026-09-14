package com.mktplace.commons.admin;

import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.events.EventContextHolder;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Protege /admin/** com o header X-Admin-Token: ausente → 401, errado → 403 (Problem Details).
 * Comparação em tempo constante para não vazar o token por timing.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class AdminTokenFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-Admin-Token";

    private final MktplaceProperties props;
    private final JsonMapper json;

    public AdminTokenFilter(MktplaceProperties props, JsonMapper json) {
        this.props = props;
        this.json = json;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/admin");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String token = request.getHeader(HEADER);
        if (token == null || token.isBlank()) {
            reject(request, response, 401, "admin-token-missing", "Header X-Admin-Token é obrigatório");
            return;
        }
        byte[] expected = props.adminToken().getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, token.getBytes(StandardCharsets.UTF_8))) {
            reject(request, response, 403, "admin-token-invalid", "Token de administração inválido");
            return;
        }
        chain.doFilter(request, response);
    }

    private void reject(HttpServletRequest request, HttpServletResponse response, int status, String code, String detail)
            throws IOException {
        Map<String, Object> problem = new LinkedHashMap<>();
        problem.put("type", "https://mktplace.dev/problems/" + code);
        problem.put("title", status == 401 ? "Não autenticado" : "Acesso negado");
        problem.put("status", status);
        problem.put("detail", detail);
        problem.put("instance", request.getRequestURI());
        problem.put("correlationId", MDC.get(EventContextHolder.MDC_CORRELATION_ID));
        response.setStatus(status);
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.getWriter().write(json.writeValueAsString(problem));
    }
}
