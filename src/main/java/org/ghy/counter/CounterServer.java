package org.ghy.counter;


import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import org.apache.commons.io.FileUtils;
import org.ghy.counter.rpc.GetValueRequestProcessor;
import org.ghy.counter.rpc.IncrementAndGetRequestProcessor;
import org.ghy.counter.rpc.ValueResponse;

import java.io.File;
import java.io.IOException;

public class CounterServer {

    private RaftGroupService raftGroupService;
    private Node node;
    private CounterStateMachine fsm;

    public CounterServer(final String dataPath, final String groupId, final PeerId serverId, final NodeOptions nodeOptions) throws IOException {
        //初始化路径
        FileUtils.forceMkdir(new File(dataPath));

        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());

        //注册业务处理器
        CounterService counterService = new CounterServiceImpl(this);
        rpcServer.registerProcessor(new GetValueRequestProcessor(counterService));
        rpcServer.registerProcessor(new IncrementAndGetRequestProcessor(counterService));

        //初始化状态机
        this.fsm = new CounterStateMachine();
        //设置状态机到启动参数
        nodeOptions.setFsm(this.fsm);
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");

        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer);
        this.node = this.raftGroupService.start();
    }

    public ValueResponse redirect() {
        final ValueResponse response = new ValueResponse();
        response.setSuccess(false);
        if (this.node != null) {
            final PeerId leader = this.node.getLeaderId();
            if (leader != null) {
                response.setRedirect(leader.toString());
            }
        }
        return response;
    }

    public CounterStateMachine getFsm() {
        return this.fsm;
    }

    public Node getNode() {
        return this.node;
    }
}
