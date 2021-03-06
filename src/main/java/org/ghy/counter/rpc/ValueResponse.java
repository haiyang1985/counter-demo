package org.ghy.counter.rpc;

import java.io.Serializable;

public class ValueResponse implements Serializable {
    private static final long serialVersionUID = -4220017686727146773L;
    private long value;
    private boolean success;
    private String redirect;
    private String errorMsg;

    public ValueResponse() {
        super();
    }

    public ValueResponse(long value, boolean success, String redirect, String errorMsg) {
        super();
        this.value = value;
        this.success = success;
        this.redirect = redirect;
        this.errorMsg = errorMsg;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getRedirect() {
        return redirect;
    }

    public void setRedirect(String redirect) {
        this.redirect = redirect;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    @Override
    public String toString() {
        return "ValueResponse [value=" + this.value + ", success=" + this.success + ", redirect=" + this.redirect
                + ", errorMsg=" + this.errorMsg + "]";
    }
}
