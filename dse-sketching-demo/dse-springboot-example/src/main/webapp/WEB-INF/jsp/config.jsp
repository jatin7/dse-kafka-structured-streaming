<html>

   <link type="text/css" rel="stylesheet" href="js/bootstrap/bootstrap.min.css" />



    <!--<script src="http://code.jquery.com/jquery-1.11.1.min.js"></script>-->
   <script src="js/jquery/jquery-1.11.1.min.js"></script>

    <script src="js/d3/d3.v4.min.js"></script>

    <!--<link rel="stylesheet" href="http://code.jquery.com/mobile/1.4.5/jquery.mobile-1.4.5.min.css" />-->

    <!--<script type="text/javascript" src="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>-->
    <script type="text/javascript" src="js/bootstrap/bootstrap.min.js"></script>


   <script type="text/javascript" src="js/datastax/initializeconfig.js"></script>

    <script type="text/javascript">

        /* Initialize Button Events and map to action*/

    </script>
</head>
<body>
    <div style="padding:50px">

        <div class="panel-group">
            <div class="panel panel-default">
                <div class="panel-heading">
                    <h4 class="panel-title sepanel-title">
                        <a data-toggle="collapse" href="#collapseCred">Twitter Credentials</a>
                    </h4>
                </div>
                <div id="collapseCred" style="margin:10px" class="panel-collapse collapse in" >
                    <table style="width:800px;border:10px;border-collapse:separate;border-spacing:5px" >
                        <tr>
                            <td>
                                <label>Consumer Key (API Key)</label>
                            </td>
                            <td>
                                <input type="text" style="width:550px" name="ConsumerKeyText" id="ConsumerKeyText"/>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <label>Consumer Secret (API Secret)</label>
                            </td>
                            <td>
                                <input type="text" style="width:550px" name="ConsumerSecretText" id="ConsumerSecretText"/>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <label>Access Token</label>
                            </td>
                            <td>
                                <input type="text" style="width:550px" name="AccessTokenText" id="AccessTokenText"/>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <label>Access Token Secret</label>
                            </td>
                            <td>
                                <input type="text" style="width:550px" name="AccessTokenSecretText" id="AccessTokenSecretText"/>
                            </td>
                        </tr>
                        <tr>
                            <td>

                            </td>
                            <td>
                                <button name="StartStreaming" id="StartStreamingButton">Start Streaming</button>
                            </td>
                        </tr>
                        <tr>
                            <td></td>
                            <td>
                                <label id="credentialslabel"></label>
                            </td>
                        </tr>
                        <tr>
                            <td></td>
                            <td>
                                <label id="resultslabel"></label>
                            </td>
                        </tr>
                    </table>


                    <h4>Don't Have a Key? Sign up its free</h4>

                    <p>
                    This demo use the Twitter Streaming API. To use this demo, you will need to sign-up for the Twitter Developer API to gain the appropriate credentials. You will need an API key, API secret, Access token, and Access token secret, follow the steps below:
                    </p>

                    <ul>
                        <li>Go to <a href="https://apps.twitter.com/" target="_blank">https://apps.twitter.com/</a> and log in with your twitter credentials.</li>
                        <li>Click "Create New App"</li>
                        <li>Fill out the form, agree to the terms, and click "Create your Twitter Application"</li>
                        <li>In the next page, click on "API keys" tab, and copy your "API key" and "API secret".</li>
                        <li>Scroll down and click "Create my access token", and copy your "Access token" and "Access token secret". (This application requires "read only" access)</li>
                        <li>Not working? For more detailed instructions: <a href="http://docs.inboundnow.com/guide/create-twitter-application/" target="_blank">See Create Twitter App Blog</a> </li>
                    </ul>
                </div>
            </div>
        </div>


    </div>



</body>
</html>
