package org.ghy.counter.snapshot;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class CounterSnapshotFile {
    private static final Logger logger = LoggerFactory.getLogger(CounterSnapshotFile.class);

    private String path;

    public CounterSnapshotFile(String path) {
        super();
        this.path = path;
    }

    public boolean save(final long value) {
        try {
            FileUtils.writeStringToFile(new File(path), String.valueOf(value));
            return true;
        } catch (IOException e) {
            logger.error("fail to save snapshot", e);
            return false;
        }
    }

    public long load() throws IOException {
        final String s = FileUtils.readFileToString(new File(path));
        if (!StringUtils.isEmpty(s)) {
            return Long.parseLong(s);
        }
        throw new IOException("Fail to load snapshot from " + path + ",content: " + s);
    }

    public String getPath() {
        return this.path;
    }
}
