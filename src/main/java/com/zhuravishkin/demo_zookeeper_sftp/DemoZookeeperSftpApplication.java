package com.zhuravishkin.demo_zookeeper_sftp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhuravishkin.demo_zookeeper_sftp.config.DomainProperties;
import com.zhuravishkin.demo_zookeeper_sftp.config.SftpConfiguration;
import com.zhuravishkin.demo_zookeeper_sftp.domain.SftpConnector;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@RequiredArgsConstructor
@SpringBootApplication
public class DemoZookeeperSftpApplication implements CommandLineRunner {
    private final DomainProperties properties;

    public static void main(String[] args) {
        SpringApplication.run(DemoZookeeperSftpApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        new Thread(() -> {
            try {
                SftpConnector sftpConnector = new SftpConnector();
                String configuration = Files.readString(Path.of("src/main/resources/static/configuration.json"));
                SftpConfiguration sftpConfiguration = mapper.readValue(configuration, SftpConfiguration.class);
                System.out.println(sftpConfiguration);
                System.out.println(properties);
                sftpConnector.setSftpConfiguration(sftpConfiguration);
                sftpConnector.setProperties(properties);
                sftpConnector.connect();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                SftpConnector sftpConnector = new SftpConnector();
                String configuration = Files.readString(Path.of("src/main/resources/static/configuration2.json"));
                SftpConfiguration sftpConfiguration = mapper.readValue(configuration, SftpConfiguration.class);
                System.out.println(sftpConfiguration);
                System.out.println(properties);
                sftpConnector.setSftpConfiguration(sftpConfiguration);
                sftpConnector.setProperties(properties);
                sftpConnector.connect();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }
}
