package org.ghy.counter;

import java.io.Serializable;

public class CounterOperation implements Serializable {
    private static final long serialVersionUID = -6597003954824547294L;

    public static final byte GET = 0x01;
    public static final byte INCREMENT = 0x02;

    private byte op;
    private long delta;

    public CounterOperation(byte op) {
        this(op, 0);
    }

    public CounterOperation(byte op, long delta) {
        this.op = op;
        this.delta = delta;
    }

    public static CounterOperation createGet() {
        return new CounterOperation(GET);
    }

    public static CounterOperation createIncrement(final long delta) {
        return new CounterOperation(INCREMENT, delta);
    }

    public byte getOp() {
        return op;
    }

    public long getDelta() {
        return delta;
    }
}
