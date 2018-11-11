package com.adbhut.demo.file;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.config.EnableIntegration;

@SpringBootApplication
@EnableIntegration
@EnableBatchProcessing
@IntegrationComponentScan
public class SpringIntegrationDemo1Application {

	public static void main(String[] args) {
		SpringApplication.run(SpringIntegrationDemo1Application.class, args);
	}
}
