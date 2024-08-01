package com.zhuravishkin.demo_zookeeper_sftp.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SftpConfiguration(@JsonProperty(value = "sftp_host") String host,
                                @JsonProperty(value = "sftp_port") Integer port,
                                @JsonProperty(value = "user") String user,
                                @JsonProperty(value = "password") String password,
                                @JsonProperty(value = "remote_file_path") String remoteFilePath,
                                @JsonProperty(value = "file_extension") String fileExtension,
                                @JsonProperty(value = "skip_row") Integer skipRow,
                                @JsonProperty(value = "file_count") Integer fileCount,
                                @JsonProperty(value = "delimiter") String delimiter,
                                @JsonProperty(value = "is_recursively") Boolean isRecursively,
                                @JsonProperty(value = "executor_initial_delay_sec") Integer initialDelay,
                                @JsonProperty(value = "executor_period_sec") Integer period,
                                @JsonProperty(value = "zk_connection") String zkConnection,
                                @JsonProperty(value = "lock_name") String lockName) {
}
