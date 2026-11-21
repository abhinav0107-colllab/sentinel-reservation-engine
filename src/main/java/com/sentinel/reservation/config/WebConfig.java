package com.sentinel.reservation.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final ApiKeyInterceptor apiKey;

    public WebConfig(ApiKeyInterceptor apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // Guest-facing endpoints (availability, reservations) are public in this demo; inventory
        // management and audit are admin-only.
        registry.addInterceptor(apiKey).addPathPatterns("/api/v1/admin/**");
    }
}
