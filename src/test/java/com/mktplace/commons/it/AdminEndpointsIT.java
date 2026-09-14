package com.mktplace.commons.it;

import com.mktplace.commons.admin.AdminTokenFilter;
import com.mktplace.commons.events.EventHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class AdminEndpointsIT extends AbstractKafkaIT {

    record Reply(int status, String contentType, String body, String correlationId) {
    }

    @Value("${local.server.port}")
    int port;

    private Reply call(String method, String path, String token, String correlationId) {
        var request = RestClient.create("http://localhost:" + port).method(org.springframework.http.HttpMethod.valueOf(method)).uri(path);
        if (token != null) {
            request = request.header(AdminTokenFilter.HEADER, token);
        }
        if (correlationId != null) {
            request = request.header(EventHeaders.HTTP_CORRELATION_ID, correlationId);
        }
        return request.exchange((req, res) -> new Reply(res.getStatusCode().value(),
                String.valueOf(res.getHeaders().getContentType()), res.bodyTo(String.class),
                res.getHeaders().getFirst(EventHeaders.HTTP_CORRELATION_ID)));
    }

    @Test
    void semTokenDa401EComTokenErrado403EmProblemJson() {
        Reply missing = call("GET", "/admin/chaos/status", null, null);
        assertThat(missing.status()).isEqualTo(401);
        assertThat(missing.contentType()).startsWith(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        assertThat(missing.body()).contains("admin-token-missing");

        Reply wrong = call("GET", "/admin/chaos/status", "nope", null);
        assertThat(wrong.status()).isEqualTo(403);
        assertThat(wrong.body()).contains("admin-token-invalid");
    }

    @Test
    void pausaERetomaSoOsListenersDeNegocio() {
        Reply paused = call("POST", "/admin/chaos/pause", "secret-test-token", null);
        assertThat(paused.status()).isEqualTo(200);
        assertThat(paused.body()).contains("\"paused\":true").contains(TestOrderListener.LISTENER_ID)
                .doesNotContain("internal-audit-tap");

        Reply state = call("GET", "/admin/consumer", "secret-test-token", null);
        assertThat(state.body()).contains("\"state\":\"PAUSED\"").contains(TestOrderListener.GROUP);

        Reply resumed = call("POST", "/admin/chaos/resume", "secret-test-token", null);
        assertThat(resumed.body()).contains("\"paused\":false");
    }

    @Test
    void correlationIdEhEcoadoOuGerado() {
        Reply echoed = call("GET", "/admin/chaos/status", "secret-test-token", "minha-correlacao");
        assertThat(echoed.correlationId()).isEqualTo("minha-correlacao");

        Reply generated = call("GET", "/admin/chaos/status", "secret-test-token", null);
        assertThat(generated.correlationId()).isNotBlank();
    }

    @Test
    void errosDeApiSaemComoProblemDetailsComCodigoECorrelationId() {
        Reply notFound = call("GET", "/admin/dlq/" + UUID.randomUUID(), "secret-test-token", "corr-404");
        assertThat(notFound.status()).isEqualTo(404);
        assertThat(notFound.contentType()).startsWith(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        assertThat(notFound.body()).contains("dlq-message-not-found").contains("\"correlationId\":\"corr-404\"");

        Reply badParam = call("POST", "/admin/chaos/fail-next?count=abc", "secret-test-token", null);
        assertThat(badParam.status()).isEqualTo(400);
        assertThat(badParam.contentType()).startsWith(MediaType.APPLICATION_PROBLEM_JSON_VALUE);

        Reply list = call("GET", "/admin/dlq", "secret-test-token", null);
        assertThat(list.status()).isEqualTo(200);
        assertThat(list.body()).contains("\"data\"").contains("\"page\"");
    }
}
