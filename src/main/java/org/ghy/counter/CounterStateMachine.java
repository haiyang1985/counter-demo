package org.ghy.counter;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import org.ghy.counter.snapshot.CounterSnapshotFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static org.ghy.counter.CounterOperation.GET;
import static org.ghy.counter.CounterOperation.INCREMENT;

public class CounterStateMachine extends StateMachineAdapter {
    private static final Logger logger = LoggerFactory.getLogger(CounterStateMachine.class);

    private final AtomicLong value = new AtomicLong(0);
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    @Override
    public void onApply(Iterator iterator) {
        while (iterator.hasNext()) {
            long current = 0;
            CounterOperation counterOperation = null;
            CounterClosure counterClosure = null;

            if (iterator.done() != null) {
                counterClosure = (CounterClosure) iterator.done();
                counterOperation = counterClosure.getCounterOperation();
            } else {
                final ByteBuffer data = iterator.getData();
                try {
                    counterOperation = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(data.array(), CounterOperation.class.getName());
                } catch (CodecException e) {
                    logger.error("Fail to decode IncrementAndGetRequest", e);
                }
            }

            if (counterOperation != null) {
                switch (counterOperation.getOp()) {
                    case GET:
                        current = this.value.get();
                        logger.info("Get value={} at logIndex={}", current, iterator.getIndex());
                        break;
                    case INCREMENT:
                        final long delta = counterOperation.getDelta();
                        final long prev = this.value.get();
                        current = this.value.addAndGet(delta);
                        logger.info("Added value={} by delta={} at logIndex={}", prev, delta, iterator.getIndex());
                        break;
                }

                if (counterClosure != null) {
                    counterClosure.success(current);
                    counterClosure.run(Status.OK());
                }
            }
            iterator.next();
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        final long currVal = this.value.get();
        Utils.runInThread(() -> {
            final CounterSnapshotFile snapshot = new CounterSnapshotFile(writer.getPath() + File.separator + "data");
            if (snapshot.save(currVal)) {
                if (writer.addFile("data")) {
                    done.run(Status.OK());
                } else {
                    done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
                }
            } else {
                done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
            }
        });
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            logger.warn("Leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta("data") == null) {
            logger.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        final CounterSnapshotFile snapshot = new CounterSnapshotFile(reader.getPath() + File.separator + "data");
        try {
            this.value.set(snapshot.load());
            return true;
        } catch (final IOException e) {
            logger.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }
    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);
    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

    @Override
    public void onError(final RaftException e) {
        logger.error("Raft error: {}", e, e);
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    public long getValue() {
        return this.value.get();
    }
}
