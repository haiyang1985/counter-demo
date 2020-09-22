package org.ghy.counter.rpc;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.ghy.counter.CounterClosure;
import org.ghy.counter.CounterService;

public class GetValueRequestProcessor implements RpcProcessor<GetValueRequest> {
    private final CounterService counterService;

    public GetValueRequestProcessor(CounterService counterService) {
        super();
        this.counterService = counterService;
    }

    @Override
    public void handleRequest(RpcContext rpcContext, GetValueRequest request) {
        final CounterClosure closure = new CounterClosure() {
            @Override
            public void run(Status status) {
                rpcContext.sendResponse(getValueResponse());
            }
        };
        this.counterService.get(request.isReadOnlySafe(), closure);
    }

    @Override
    public String interest() {
        return GetValueRequest.class.getName();
    }
}
