package com.zhuravishkin.demo_zookeeper_sftp.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@ToString
@Getter
@Setter
@Component
//@PropertySource("classpath:application.yml")
@ConfigurationProperties(prefix = "service")
public class DomainProperties {
    private String nodesPath;
    private String lockPath;
    private Integer sessionTimeoutMs;
    private Integer connectionTimeoutMs;
    private Integer delayedStartSec;
}
