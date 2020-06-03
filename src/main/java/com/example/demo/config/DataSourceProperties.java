package com.example.demo.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties(prefix = DataSourceProperties.DS, ignoreUnknownFields = false)
@Getter
@Setter
public class DataSourceProperties {

    final static String DS = "spring.datasource";

    private Map<String,String> mysqlMain;

    private Map<String,String> hive;

    private Map<String,String> commonConfig;


}
