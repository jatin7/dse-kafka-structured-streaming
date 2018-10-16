package com.dse.se.dto;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by michaelraney on 3/7/18.
 */
public class UniqueUsersDTO implements Serializable {

    private Long resultTime;

    private Approximate[] approximates;

    public Long getResultTime() {
        return resultTime;
    }

    public void setResultTime(Long resultTime) {
        this.resultTime = resultTime;
    }

    public Approximate[] getApproximates() {
        return approximates;
    }

    public void setApproximates(Approximate[] approximates) {
        this.approximates = approximates;
    }
}
