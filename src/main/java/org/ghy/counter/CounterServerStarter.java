package org.ghy.counter;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Component
public class CounterServerStarter {
    private static final Logger logger = LoggerFactory.getLogger(CounterServerStarter.class);

    @PostConstruct
    public void init() {
        String dataPath = "/tmp/server1";
        String groupId = "counter";
        String serverIdStr = "127.0.0.1:8082";
        String initConfStr = "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083";

        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setElectionTimeoutMs(1000);
        nodeOptions.setDisableCli(false);
        nodeOptions.setSnapshotIntervalSecs(30);

        final PeerId serverId = new PeerId();
        if (!serverId.parse(serverIdStr)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr);
        }
        final Configuration initConf = new Configuration();
        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        nodeOptions.setInitialConf(initConf);

        try {
            final CounterServer counterServer = new CounterServer(dataPath, groupId, serverId, nodeOptions);
            logger.info("Counter server started, serverId: %s" + serverId);
        } catch (IOException e) {
            logger.error("Fail to start counter server.", e);
        }
    }
}
