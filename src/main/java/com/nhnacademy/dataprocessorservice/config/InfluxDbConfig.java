package com.nhnacademy.dataprocessorservice.config;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class InfluxDbConfig {
    @Value("${influxdb.url}")
    private String url;

    @Value("${influxdb.token}")
    private String token;

    @Value("${influxdb.org}")
    private String org;

    @Value("${influxdb.bucket}")
    private String bucket;

    @Bean
    public InfluxDBClient influxDBClient() {

        return InfluxDBClientFactory.create(
                url,
                token.toCharArray(),
                org,
                bucket
        );
    }
    @Bean
    public WriteApi writeApi(InfluxDBClient influxDBClient) {
        return influxDBClient.getWriteApi(
                WriteOptions.builder()
                        .batchSize(100)
                        .flushInterval(500)
                        .maxRetries(5)
                        .retryInterval(5000)
                        .build()
        );
    }

}
