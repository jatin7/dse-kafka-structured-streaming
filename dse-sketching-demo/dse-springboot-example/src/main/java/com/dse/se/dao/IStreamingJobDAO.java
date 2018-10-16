package com.dse.se.dao;

/**
 * Created by michaelraney on 4/7/18.
 */
public interface IStreamingJobDAO {

    void startStreaming(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) throws Exception;

}