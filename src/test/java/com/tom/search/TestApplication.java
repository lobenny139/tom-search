package com.tom.search;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

@SpringBootApplication

@ComponentScan(basePackages = {
        //local
        "com.tom.search.service.provider"
})

@Import({
        com.tom.search.beanServiceConfig.ElasticSearchConfig.class
})
public class TestApplication {
    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }
}