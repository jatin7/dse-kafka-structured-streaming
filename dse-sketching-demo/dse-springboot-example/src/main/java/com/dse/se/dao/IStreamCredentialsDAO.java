package com.dse.se.dao;

import twitter4j.TwitterException;

/**
 * Created by michaelraney on 4/7/18.
 */
public interface IStreamCredentialsDAO {

     void validateCredentials(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) throws Exception;

}
