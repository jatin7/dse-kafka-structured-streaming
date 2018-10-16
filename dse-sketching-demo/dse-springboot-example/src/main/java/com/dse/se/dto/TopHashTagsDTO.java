package com.dse.se.dto;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by michaelraney on 3/8/18.
 */
public class TopHashTagsDTO implements Serializable {

    private Long resultTime;

    private Map<String, Long> topTags;

    public Long getResultTime() {
        return resultTime;
    }

    public void setResultTime(Long resultTime) {
        this.resultTime = resultTime;
    }

    public Map<String, Long> getTopTags() {
        return topTags;
    }

    public void setTopTags(Map<String, Long> topTags) {
        this.topTags = topTags;
    }
}
