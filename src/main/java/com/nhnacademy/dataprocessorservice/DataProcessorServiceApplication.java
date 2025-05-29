package com.nhnacademy.dataprocessorservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableScheduling
@EnableDiscoveryClient
@EnableAspectJAutoProxy(exposeProxy = true)
public class DataProcessorServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataProcessorServiceApplication.class, args);
    }

}
