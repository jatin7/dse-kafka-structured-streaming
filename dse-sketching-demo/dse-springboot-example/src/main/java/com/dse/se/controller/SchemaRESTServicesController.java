package com.dse.se.controller;

import com.dse.se.dto.StreamingJobDTO;
import com.dse.se.dto.TopHashTagsDTO;
import com.dse.se.dto.UniqueUsersDTO;
import com.dse.se.service.SketchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;

/***
 *  * The following API template allows the retrieving and posting of Schema document
 * to Solr Nodes via REST API
 *
 *  @implNote I would have liked to perform these request directly from the JavaScript,
 * but because of Same-Origin Policy (SOP) security measure enabled in most browsers,
 * users would most likely see 'Access-Control-Allow-Origin' errors when
 * performing such request across domains.
 *
 *@author Michael Raney
 */
@RestController
public class SchemaRESTServicesController {

    @Autowired
    SketchService sketchService;

    /***
     * Retrieve unique number of users by window
     *
     * @return uniqueusers raw format
     */
    @RequestMapping(method = RequestMethod.GET, path = "/getUniqueUsersForToday")
    public UniqueUsersDTO getUniqueUsersForToday() throws ParseException{

        return sketchService.getUniqueUsersForToday();

    }

    @RequestMapping(method = RequestMethod.GET, path = "/getUniqueUsersRollup")
    public UniqueUsersDTO getUniqueUsersRollup() throws ParseException{

        return sketchService.getUniqueUsersRollup();

    }


    @RequestMapping(method = RequestMethod.GET, path = "/getTopTweetsRollup")
    public TopHashTagsDTO getTopTweetsRollup() throws ParseException{

        return sketchService.getTopTweetsRollup();

    }



    @RequestMapping(method = RequestMethod.GET, path = "/startStreaming")
    public StreamingJobDTO startStreaming(@RequestParam(name = "consumerKey") String consumerKey,
                                          @RequestParam(name = "consumerSecret") String consumerSecret,
                                          @RequestParam(name = "accessToken") String accessToken,
                                          @RequestParam(name = "accessTokenSecret") String accessTokenSecret) throws ParseException{


        return sketchService.startStreaming(consumerKey, consumerSecret, accessToken, accessTokenSecret);

    }




        /*
    @RequestMapping(method = RequestMethod.GET, path = "/createCore")
    public String createCode(@RequestParam(name = "domain")String domain,
                                       @RequestParam(name = "schema")String schema,
                                       @RequestParam(name = "table")String table){ //@RequestParam(name = "address") String address) {

        solrSchemaDAO.createCore(domain, schema, table);

        return solrSchemaDAO.getSchemaFromAddress(domain, schema, table);

    }


    @RequestMapping(method = RequestMethod.POST, path= "/uploadNewSchema")
    public String uploadNewSchema(@RequestParam(name = "xml")String xml,
                                  @RequestParam(name = "domain")String domain,
                                  @RequestParam(name = "schema")String schema,
                                  @RequestParam(name = "table")String table) {


       return solrSchemaDAO.uploadNewSchema(xml, domain, schema, table);
    }


    @RequestMapping(method = RequestMethod.GET, path= "/reloadCore")
    public String reloadCore(@RequestParam(name = "domain")String domain,
                             @RequestParam(name = "schema")String schema,
                             @RequestParam(name = "table")String table){

        return solrSchemaDAO.reloadCore(domain, schema, table);
    }

*/


}


