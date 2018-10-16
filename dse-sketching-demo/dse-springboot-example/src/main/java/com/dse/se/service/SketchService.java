package com.dse.se.service;

import com.dse.se.dao.IStreamCredentialsDAO;
import com.dse.se.dao.IStreamingJobDAO;
import com.dse.se.dao.ISketchTimeSeriesDAO;
import com.dse.se.dto.StreamingJobDTO;
import com.dse.se.dto.TopHashTagsDTO;
import com.dse.se.dto.UniqueUsersDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.util.Date;

/**
 * Created by michaelraney on 3/18/18.
 */
@Component
public class SketchService {

    @Autowired
    ISketchTimeSeriesDAO sketchTimeSeriesDAO;

    @Autowired
    IStreamingJobDAO streamingJobDAO;

    @Autowired
    IStreamCredentialsDAO streamCredentialsDAO;

    Integer elapsedTimeInMills = 1000 * 60 * 60 * 1;//One Hour

    public UniqueUsersDTO getUniqueUsersForToday() throws ParseException {

        Date now = new Date();
        Date relativeElapsedTime = new Date(now.getTime() - elapsedTimeInMills);
        return sketchTimeSeriesDAO.getUniqueUsersForToday(now, relativeElapsedTime);

    }

    public UniqueUsersDTO getUniqueUsersRollup() throws ParseException {

        Date now = new Date();
        Date relativeElapsedTime = new Date(now.getTime() - elapsedTimeInMills);

        return sketchTimeSeriesDAO.getUniqueUsersRollup(now, relativeElapsedTime);
    }

    public TopHashTagsDTO getTopTweetsRollup() throws ParseException {
        return sketchTimeSeriesDAO.getTopTweetsRollup(new Date());

    }


    public StreamingJobDTO startStreaming(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret){

        StreamingJobDTO streamingJobDTO = new StreamingJobDTO();
        streamingJobDTO.setValidated(false);
        try {
            streamCredentialsDAO.validateCredentials(consumerKey, consumerSecret, accessToken, accessTokenSecret);

            streamingJobDTO.setValidated(true);


            streamingJobDAO.startStreaming(consumerKey, consumerSecret, accessToken, accessTokenSecret);

            streamingJobDTO.setMessage("Validated Credentials and Started Streaming Script");
        }catch(Exception ex){
            ex.printStackTrace();
            streamingJobDTO.setMessage(ex.getMessage());
        }

        return streamingJobDTO;
    }
}