package org.ghy.counter;

import com.alipay.sofa.jraft.Closure;
import org.ghy.counter.rpc.ValueResponse;

public abstract class CounterClosure implements Closure {
    private ValueResponse valueResponse;
    private CounterOperation counterOperation;

    public ValueResponse getValueResponse() {
        return valueResponse;
    }

    public void setValueResponse(ValueResponse valueResponse) {
        this.valueResponse = valueResponse;
    }

    public CounterOperation getCounterOperation() {
        return counterOperation;
    }

    public void setCounterOperation(CounterOperation counterOperation) {
        this.counterOperation = counterOperation;
    }

    protected void failure(final String errorMsg, final String redirect) {
        final ValueResponse response = new ValueResponse();
        response.setSuccess(false);
        response.setErrorMsg(errorMsg);
        response.setRedirect(redirect);
        setValueResponse(response);
    }

    protected void success(final long value) {
        final ValueResponse response = new ValueResponse();
        response.setSuccess(true);
        response.setValue(value);
        setValueResponse(response);
    }
}
