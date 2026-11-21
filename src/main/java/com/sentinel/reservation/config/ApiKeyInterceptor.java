package com.sentinel.reservation.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Minimal protection for admin endpoints: a shared secret in
 * {@code X-Api-Key}. Deliberately simple for a single-tenant demo; a production system would use
 * OAuth2 client credentials per partner. Fails closed: if no key is configured, protected endpoints
 * are rejected. Comparison is constant-time to avoid timing side channels.
 */
@Component
public class ApiKeyInterceptor implements HandlerInterceptor {

    public static final String HEADER = "X-Api-Key";
    private final byte[] expected;

    public ApiKeyInterceptor(SentinelProperties props) {
        String key = props.admin() == null ? null : props.admin().apiKey();
        this.expected = key == null || key.isBlank() ? null : key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String provided = request.getHeader(HEADER);
        if (expected != null && provided != null
                && MessageDigest.isEqual(expected, provided.getBytes(StandardCharsets.UTF_8))) {
            return true;
        }
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        return false;
    }
}
