package com.dse.se.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;


/***
 *
 *  Default Context for Application
 *
 * @Michael Raney
 */
@Controller
public class WebViewController {

    @RequestMapping("/")
    public String index() {
        return "index";
    }

    @RequestMapping("/config")
    public String config() {
        return "config";
    }
}


