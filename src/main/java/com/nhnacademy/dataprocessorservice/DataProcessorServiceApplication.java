package com.nhnacademy.dataprocessorservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableScheduling
@EnableDiscoveryClient
public class DataProcessorServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataProcessorServiceApplication.class, args);
    }

}
