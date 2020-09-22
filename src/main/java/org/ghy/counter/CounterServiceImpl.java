package org.ghy.counter;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.StoreEngineHelper;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

public class CounterServiceImpl implements CounterService {
    private static final Logger logger = LoggerFactory.getLogger(CounterServiceImpl.class);

    private final CounterServer counterServer;
    private final Executor readIndexExecutor;

    public CounterServiceImpl(CounterServer counterServer) {
        this.counterServer = counterServer;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    @Override
    public void get(boolean readOnlySafe, CounterClosure closure) {
        if (!readOnlySafe) {
            closure.success(getValue());
            closure.run(Status.OK());
        }

        this.counterServer.getNode().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
            @Override
            public void run(Status status, long l, byte[] bytes) {
                if (status.isOk()) {
                    closure.success(getValue());
                    closure.run(Status.OK());
                    return;
                }
                CounterServiceImpl.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        applyOperation(CounterOperation.createGet(), closure);
                    } else {
                        handlerNotLeaderError(closure);
                    }
                });
            }
        });
    }

    @Override
    public void incrementAndGet(long delta, CounterClosure closure) {
        applyOperation(CounterOperation.createIncrement(delta), closure);
    }

    private Executor createReadIndexExecutor() {
        final StoreEngineOptions opts = new StoreEngineOptions();
        return StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads());
    }

    private void applyOperation(final CounterOperation op, final CounterClosure closure) {
        if (!isLeader()) {
            handlerNotLeaderError(closure);
            return;
        }

        try {
            closure.setCounterOperation(op);
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(op)));
            task.setDone(closure);
            this.counterServer.getNode().apply(task);
        } catch (CodecException e) {
            String errorMsg = "Fail to encode CounterOperation";
            logger.error(errorMsg, e);
            closure.failure(errorMsg, StringUtils.EMPTY);
            closure.run(new Status(RaftError.EINTERNAL, errorMsg));
        }
    }

    private void handlerNotLeaderError(final CounterClosure closure) {
        closure.failure("Not leader.", getRedirect());
        closure.run(new Status(RaftError.EPERM, "Not leader"));
    }

    private String getRedirect() {
        return this.counterServer.redirect().getRedirect();
    }

    private long getValue() {
        return this.counterServer.getFsm().getValue();
    }

    private boolean isLeader() {
        return this.counterServer.getFsm().isLeader();
    }
}
