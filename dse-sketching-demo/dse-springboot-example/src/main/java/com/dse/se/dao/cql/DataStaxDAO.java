package com.dse.se.dao.cql;

import com.datastax.driver.core.*;
import com.dse.se.dao.ISketchTimeSeriesDAO;
import com.dse.se.dto.Approximate;
import com.dse.se.dto.TopHashTagsDTO;
import com.dse.se.dto.UniqueUsersDTO;
import com.google.common.base.Stopwatch;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;



@Component
public class DataStaxDAO implements ISketchTimeSeriesDAO {

    //yyyy-mm-dd HH:mm:ssZ
    private static final SimpleDateFormat resultFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");


    public static void main (String[] args) throws ParseException{
        new DataStaxDAO().getUniqueUsersForToday(dateFormat.parse("03-18-2018"), resultFormat.parse("2018-03-18 04:37:50+0000"));//last 5 minutes
    }

    @Override
    public UniqueUsersDTO getUniqueUsersForToday(Date day, Date relativeElapsedTime) throws ParseException {

        Session session = DataStaxSessionFactory.getInstance().getSession();

        String dayAsString = dateFormat.format(day);

        PreparedStatement preparedUniqueUsersForToday = DataStaxSessionFactory.getInstance().getPreparedUniqueUsersForToday();

        BoundStatement bound = preparedUniqueUsersForToday.bind("uniqueusers", dayAsString, Timestamp.from(relativeElapsedTime.toInstant()));

        Stopwatch stopwatch = Stopwatch.createStarted();
        ResultSet results = session.execute(bound);
        stopwatch.stop();

        List<Approximate> approximateList = new ArrayList<>(results.getAvailableWithoutFetching());

        //TODO Use Object Mapper
        for(Row oneResult : results){

            Date time = oneResult.getTimestamp(0);
            Integer uniqueUsers = oneResult.getInt(1);

            Approximate oneApproximate = new Approximate();
            oneApproximate.setTime(resultFormat.format(time));
            oneApproximate.setUniqueUsers(uniqueUsers);

            approximateList.add(oneApproximate);
        }

        UniqueUsersDTO uniqueUsersDTO = new UniqueUsersDTO();
        uniqueUsersDTO.setApproximates(approximateList.toArray(new Approximate[approximateList.size()]));
        uniqueUsersDTO.setResultTime(stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return uniqueUsersDTO;
    }
    @Override
    public UniqueUsersDTO getUniqueUsersRollup(Date day, Date relativeElapsedTime) throws ParseException {

        Session session = DataStaxSessionFactory.getInstance().getSession();

        String dayAsString = dateFormat.format(day);


        PreparedStatement preparedUniqueUsersRollup = DataStaxSessionFactory.getInstance().getPreparedUniqueUsersRollup();


        BoundStatement bound = preparedUniqueUsersRollup.bind("uniqueusersrollup",  dayAsString, Timestamp.from(relativeElapsedTime.toInstant()));

        Stopwatch stopwatch = Stopwatch.createStarted();
        ResultSet results = session.execute(bound);
        stopwatch.stop();

        List<Approximate> approximateList = new ArrayList<>(results.getAvailableWithoutFetching());

        //TODO Use Object Mapper
        for(Row oneResult : results){

            Date time = oneResult.getTimestamp(0);
            Integer uniqueUsers = oneResult.getInt(1);

            Approximate oneApproximate = new Approximate();
            oneApproximate.setTime(resultFormat.format(time));
            oneApproximate.setUniqueUsers(uniqueUsers);

            approximateList.add(oneApproximate);
        }

        UniqueUsersDTO uniqueUsersDTO = new UniqueUsersDTO();
        uniqueUsersDTO.setApproximates(approximateList.toArray(new Approximate[approximateList.size()]));
        uniqueUsersDTO.setResultTime(stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return uniqueUsersDTO;
    }
    @Override
    public TopHashTagsDTO getTopTweetsRollup(Date day) throws ParseException{

        Session session = DataStaxSessionFactory.getInstance().getSession();

        String dayAsString = dateFormat.format(day);

        PreparedStatement preparedTopTweetsRollup = DataStaxSessionFactory.getInstance().getPreparedTopTweetsRollup();

        BoundStatement bound = preparedTopTweetsRollup.bind("tophashtagsrollup", dayAsString);

        Stopwatch stopwatch = Stopwatch.createStarted();
        ResultSet results = session.execute(bound);
        stopwatch.stop();


        //Integer count  = results.getAvailableWithoutFetching();

        TopHashTagsDTO topHashTagsDTO = new TopHashTagsDTO();
        topHashTagsDTO.setResultTime(stopwatch.elapsed(TimeUnit.MILLISECONDS));

        results.forEach(row -> {
            Date time = row.getTimestamp(0);

            System.out.println(time);

            Map<String, Long> map = row.getMap(1, String.class, Long.class);

            topHashTagsDTO.setTopTags(map);
        });



        return topHashTagsDTO;
    }
}