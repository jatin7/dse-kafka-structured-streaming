package com.dse.se.dao;

import com.dse.se.dto.TopHashTagsDTO;
import com.dse.se.dto.UniqueUsersDTO;

import java.text.ParseException;
import java.util.Date;

public interface ISketchTimeSeriesDAO {

     UniqueUsersDTO getUniqueUsersForToday(Date day, Date relativeElapsedTime) throws ParseException;

     UniqueUsersDTO getUniqueUsersRollup(Date day, Date relativeElapsedTime)throws ParseException;

     TopHashTagsDTO getTopTweetsRollup(Date day)throws ParseException;

}
