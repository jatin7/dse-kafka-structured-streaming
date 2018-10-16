<html>

    <!--<link href="https://fonts.googleapis.com/css?family=Open+Sans:400,300,700" rel="stylesheet" type="text/css">-->
    <link type="text/css" rel="stylesheet" href="js/fonts/opensans.css" rel="stylesheet" type="text/css"/>

    <!--<link href="https://fonts.googleapis.com/css?family=PT+Serif:400,700,400italic" rel="stylesheet" type="text/css">-->
    <link type="text/css" rel="stylesheet" href="js/fonts/ptserif.css" rel="stylesheet" type="text/css"/>

    <!--<link href="https://netdna.bootstrapcdn.com/font-awesome/4.2.0/css/font-awesome.css" rel="stylesheet" type="text/css">-->
    <link type="text/css" rel="stylesheet" href="js/fonts/font-awesome.css" rel="stylesheet" type="text/css"/>
    <!--<link href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css" rel="stylesheet" type="text/css">-->
    <link type="text/css" rel="stylesheet" href="js/bootstrap/bootstrap.min.css" />

    <!--<link type="text/css" rel="stylesheet" href="https://netdna.bootstrapcdn.com/font-awesome/4.2.0/fonts/fontawesome-webfont.woff?v=4.2.0" rel="stylesheet" type="text/css"/>-->
    <link type="text/css" rel="stylesheet" href="js/fonts/fontawesome-webfont.woff?v=4.2.0" rel="stylesheet" type="text/css"/>

    <link type="text/css" rel="stylesheet" href="js/metricsjs/metricsgraphics.css"/>
    <link type="text/css" rel="stylesheet" href="js/metricsjs/metricsgraphics-demo-dark.css"/>

    <link type="text/css" rel="stylesheet" href="js/metricsjs/highlightjs-default.css"/>
    <link type="text/css" rel="stylesheet" href="js/metricsjs/railscasts.css"/>

    <!--<script src="http://code.jquery.com/jquery-1.11.1.min.js"></script>-->
   <script src="js/jquery/jquery-1.11.1.min.js"></script>

    <script src="js/d3/d3.v4.min.js"></script>

    <!--<script type="text/javascript" src="js/metricsjs/metricsgraphics.js"></script>-->
    <script type="text/javascript" src="js/metricsjs/metricsgraphics.min.js"></script>

    <link href='' rel='stylesheet' type='text/css' id='dark'>
    <link href='' rel='stylesheet' type='text/css' id='dark-code'>

    <!--<link rel="stylesheet" href="http://code.jquery.com/mobile/1.4.5/jquery.mobile-1.4.5.min.css" />-->

    <!--<script type="text/javascript" src="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>-->
    <script type="text/javascript" src="js/bootstrap/bootstrap.min.js"></script>

    <!--<script type="text/javascript" src="https://cdn.datatables.net/v/dt/dt-1.10.16/datatables.min.js"></script>-->
    <script type="text/javascript" src="js/bootstrap/datatables.min.js"></script>

     <link type="text/css" rel="stylesheet" href="js/datastax/trendingdemo.css"/>

     <script type="text/javascript" src="js/datastax/toptweets.js"></script>
     <script type="text/javascript" src="js/datastax/uniqueusers.js"></script>

    <script type="text/javascript" src="js/datastax/initializescript.js"></script>

    <!-- 1024×768 -->

    <script type="text/javascript">

        /* Initialize Button Events and map to action*/

    </script>
</head>
<body>
    <div class="secontainer">
        <div class="setitlediv">
            <table >
                <tr><td><img class="sedselogo" src="images/datastaxinvert.png"></td><td class="setitle">Trending Now</td>
                    <td class="setitleOptions">

                        <script language="JavaScript">
                            document.write("<a target='_blank' href='" + window.location.protocol + "//" + window.location.hostname + ":8888'" + "> <img class='seimagelinks' src='images/OptionsIcon.png'></a>" );
                        </script>
                         <script language="JavaScript">
                            document.write("<a target='_blank' href='" + window.location.protocol + "//" + window.location.hostname + ":7080'" + "> <img class='seimagelinks' src='images/analytics.png'></a>" );
                        </script>
                        <script language="JavaScript">
                            document.write("<a target='_blank' href='" + window.location.protocol + "//" + window.location.hostname + ":8080'" + "> <img class='seimagelinks' src='images/notebook.png'></a>" );
                        </script>

                        <a href="https://github.com/michaelraney/datastax-sketch-examples" target="_blank" >
                            <img class='seimagelinks' src="images/github.png">
                        </a>

                    </td>
                </tr>
            </table>
        </div>

        <div class="panel-group">
            <div class="panel panel-default">
                <div class="panel-heading">
                    <h4 class="panel-title sepanel-title" >
                        <a data-toggle="collapse" href="#collapse1">Perspective: 10 Second Window</a>
                    </h4>
                </div>
                <div id="collapse1" class="panel-collapse collapse sepanel-colapse in" >
                        <div id="mygraph">

                        </div>
                        <div class="panel-footer"><label id="uniqueUserQueryTime"></label></div>
                </div>
            </div>
        </div>

        <div class="panel-group">
            <div class="panel panel-default">
                <div class="panel-heading">
                    <h4 class="panel-title sepanel-title" >
                        <a data-toggle="collapse" href="#collapse2">Perspective: 10 Minute Window</a>
                    </h4>
                </div>
                <div id="collapse2" class="panel-collapse collapse sepanel-colapse">
                    <div class="panel-body">
                        <table>
                            <tr>
                                <td>
                                    <div width="499" id="mygraphrollup"/></td>
                                <td>
                                    <table id="toptagstable" class="table table-sm table-hover borderless setoptagstable">
                                    </table>
                                </td>

                            </tr>

                        </table>

                    </div>
                    <div class="panel-footer">
                        <table>
                            <tr>
                                <td class="seuniqueuserquerytimecell"><label id="uniqueUserRollupQueryTime"></label></td>
                                <td><label id="topTweetsQueryTime"></label></td>
                            </tr>
                        </table>
                     </div>
                </div>
            </div>
        </div>



        <div class="panel-group">
            <div class="panel panel-default">
                <div class="panel-heading">
                    <h4 class="panel-title sepanel-title">
                        <a data-toggle="collapse" href="#collapse3">Architecture Diagram</a>
                    </h4>
                </div>
                <div id="collapse3" class="panel-collapse collapse sepanel-colapse" >
                    <table>
                        <tr>
                            <td><img class="searchitectureimage" src="images/ApproximationsArchitecture.png"></td>
                        </tr>
                    </table>

                </div>
            </div>
        </div>
        <br/>
        <br/>



    </div>



</body>
</html>