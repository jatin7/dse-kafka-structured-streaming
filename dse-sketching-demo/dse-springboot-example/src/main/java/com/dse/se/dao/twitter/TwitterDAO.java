package com.dse.se.dao.twitter;

import com.dse.se.dao.IStreamCredentialsDAO;
import org.springframework.stereotype.Component;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Created by michaelraney on 4/7/18.
 */
@Component
public class TwitterDAO implements IStreamCredentialsDAO{


    public void validateCredentials(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) throws TwitterException{

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret);

        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        User status = twitter.verifyCredentials();

    }
}
