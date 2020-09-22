package org.ghy.counter.rpc;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.ghy.counter.CounterClosure;
import org.ghy.counter.CounterService;

public class IncrementAndGetRequestProcessor implements RpcProcessor<IncrementAndGetRequest> {

    private final CounterService counterService;

    public IncrementAndGetRequestProcessor(CounterService counterService) {
        super();
        this.counterService = counterService;
    }

    @Override
    public void handleRequest(RpcContext rpcContext, IncrementAndGetRequest request) {
        final CounterClosure closure = new CounterClosure() {
            @Override
            public void run(Status status) {
                rpcContext.sendResponse(getValueResponse());
            }
        };
        this.counterService.incrementAndGet(request.getDelta(), closure);
    }

    @Override
    public String interest() {
        return IncrementAndGetRequest.class.getName();
    }
}
