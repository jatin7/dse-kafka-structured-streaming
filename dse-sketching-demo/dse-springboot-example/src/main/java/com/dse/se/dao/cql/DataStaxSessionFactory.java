package com.dse.se.dao.cql;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

/**
 * Created by michaelraney on 3/5/18.
 */
public class DataStaxSessionFactory {

    public static DataStaxSessionFactory instance;

    public String seedNode = "node0";

    private DseSession session;

    private DataStaxSessionFactory(){}

    private PreparedStatement preparedUniqueUsersForToday ;
    private PreparedStatement preparedUniqueUsersRollup ;

    private PreparedStatement preparedTopTweetsRollup ;

    public static DataStaxSessionFactory getInstance() {
        if(instance == null){
            instance = getOrCreateDataStaxFactory();
        }
        return instance;
    }
    private static synchronized DataStaxSessionFactory getOrCreateDataStaxFactory(){
        if(instance == null){
            instance = new DataStaxSessionFactory();
        }
        return instance;
    }

    /*** Instance Methods ***/


    public DseSession getSession(){

        if(session == null){
            session = getOrCreateDataStaxSession();
        }
        return session;
    }



    private synchronized DseSession getOrCreateDataStaxSession() {

        if (session == null) {
            DseCluster cluster = null;
            try {
                cluster = DseCluster.builder()                                               // (1)
                        .addContactPoint(seedNode)
                        .build();

                 session = cluster.connect();                                      // (2)


                //Row row = session.execute("select release_version from system.local").one(); // (3)
                // System.out.println(row.getString("release_version"));                        // (4)
            } finally {
                //if (cluster != null) cluster.close();                                        // (5)
            }
        }
        return session;
    }

    public PreparedStatement getPreparedUniqueUsersForToday(){
        if(preparedUniqueUsersForToday == null){
            preparedUniqueUsersForToday = getOrCreatePreparedUniqueUsersForToday();
        }
        return preparedUniqueUsersForToday;
    }
    private synchronized PreparedStatement getOrCreatePreparedUniqueUsersForToday(){
        if(preparedUniqueUsersForToday == null){
             preparedUniqueUsersForToday = getSession().prepare(
                    "select batchtime, uniqueperbatch from approximations.hlldata where id = ? and date = ? and batchtime > ?");
        }
        return preparedUniqueUsersForToday;
    }

    public PreparedStatement getPreparedUniqueUsersRollup(){
        if(preparedUniqueUsersRollup == null){
            preparedUniqueUsersRollup = getOrCreatePreparedUniqueUsersRollup();
        }
        return preparedUniqueUsersRollup;
    }
    private synchronized PreparedStatement getOrCreatePreparedUniqueUsersRollup(){
        if(preparedUniqueUsersRollup == null){
            preparedUniqueUsersRollup = getSession().prepare(
                    "select batchtime, uniqueperbatch from approximations.hlldata10min where id = ? and date = ? and batchtime > ?");
        }
        return preparedUniqueUsersRollup;
    }

    public PreparedStatement getPreparedTopTweetsRollup(){
        if(preparedTopTweetsRollup == null){
            preparedTopTweetsRollup = getOrCreatePreparedTopTweetsRollup();
        }
        return preparedTopTweetsRollup;
    }
    private synchronized PreparedStatement getOrCreatePreparedTopTweetsRollup(){
        if(preparedTopTweetsRollup == null){
            preparedTopTweetsRollup = getSession().prepare(
                    "select batchtime, preview from approximations.cmsdata10min where id = ? and date = ? limit 2");
        }
        return preparedTopTweetsRollup;
    }


}
