package com.dse.se.dto;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by michaelraney on 3/7/18.
 */
public class Approximate implements Serializable{

    private Integer uniqueUsers;

    private String time;

    public Integer getUniqueUsers() {
        return uniqueUsers;
    }

    public void setUniqueUsers(Integer uniqueUsers) {
        this.uniqueUsers = uniqueUsers;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
