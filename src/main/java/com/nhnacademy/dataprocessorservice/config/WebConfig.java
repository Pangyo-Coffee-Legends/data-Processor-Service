package com.nhnacademy.dataprocessorservice.config;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.Executor;


@Configuration
public class WebConfig {
    @Bean
    public RestTemplate restTemplate(){
        return new RestTemplate();
    }


    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean(name = "modelDispatcherExecutor")
    public Executor modelDispatcherExecutor(
            @Value("${async.core-pool-size}") int corePool,
            @Value("${async.max-pool-size}") int maxPool) {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(corePool);
        exec.setMaxPoolSize(maxPool);
        exec.setThreadNamePrefix("model-dispatch-");
        exec.initialize();
        return exec;
    }
}
