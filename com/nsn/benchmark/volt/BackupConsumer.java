package com.nsn.benchmark.volt;

import com.nsn.utility.SimpleCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.Consumer;

/**
 * 文件备份
 *
 * @author zhangxu
 */
public class BackupConsumer implements Consumer<Path> {
    private static final Logger logger = LoggerFactory.getLogger(BackupConsumer.class);
    private String policy;

    /**
     * 文件备份规则
     *
     * @param policy 规则
     */
    public BackupConsumer(String policy) {
        this.policy = policy;
    }

    @Override
    public void accept(Path path) {
        try {
            SimpleCase.move(path, policy, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            logger.error("", e);
        }
    }
}
