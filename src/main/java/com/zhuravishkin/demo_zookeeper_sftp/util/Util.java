package com.zhuravishkin.demo_zookeeper_sftp.util;

import com.jcraft.jsch.ChannelSftp;
import com.zhuravishkin.demo_zookeeper_sftp.config.SftpConfiguration;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Vector;
import java.util.zip.GZIPInputStream;

@UtilityClass
public class Util {
    public static final String FILE_SEPARATOR = "/";

    @SneakyThrows
    public static void findFilesRecursively(SftpConfiguration sftpConfiguration,
                                            ChannelSftp channelSftp,
                                            String remoteFilePath,
                                            Map<String, Long> fileNameTimeMap,
                                            boolean isRecursively) {
        Vector<ChannelSftp.LsEntry> entries = channelSftp.ls(remoteFilePath);
        for (ChannelSftp.LsEntry entry : entries) {
            String filename = entry.getFilename();
            if (isRecursively && entry.getAttrs().isDir()) {
                findFilesRecursively(sftpConfiguration, channelSftp, remoteFilePath + FILE_SEPARATOR + filename, fileNameTimeMap, isRecursively);
            } else {
                if (Arrays.stream(sftpConfiguration.fileExtension().split(",")).anyMatch(filename::endsWith)) {
                    long modifyTime = entry.getAttrs().getMTime() * 1000L;
                    fileNameTimeMap.put(remoteFilePath + FILE_SEPARATOR + filename, modifyTime);
                }
            }
        }
    }

    @SneakyThrows
    public static InputStreamReader supplyInputStreamReader(String extension, InputStream inputStream) {
        if ("gz".equals(extension)) {
            GZIPInputStream gzip = new GZIPInputStream(inputStream);
            return new InputStreamReader(gzip);
        } else {
            return new InputStreamReader(inputStream);
        }
    }
}
