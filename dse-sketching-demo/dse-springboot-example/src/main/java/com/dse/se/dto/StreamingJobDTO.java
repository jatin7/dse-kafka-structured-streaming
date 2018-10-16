package com.dse.se.dto;

import java.io.Serializable;

/**
 * Created by michaelraney on 4/7/18.
 */
public class StreamingJobDTO implements Serializable {

    private String message;

    private Boolean validated;

    public String getMessage() {
        return message;
    }

    public Boolean getValidated() {
        return validated;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setValidated(Boolean validated) {
        this.validated = validated;
    }
}
