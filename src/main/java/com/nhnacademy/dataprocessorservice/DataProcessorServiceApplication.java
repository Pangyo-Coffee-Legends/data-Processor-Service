package com.nhnacademy.dataprocessorservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DataProcessorServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataProcessorServiceApplication.class, args);
    }

}
