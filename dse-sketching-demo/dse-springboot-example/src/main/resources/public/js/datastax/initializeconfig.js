/**
 * Created by michaelraney on 3/16/18.
 */


$(document).ready(function() {
    $("#StartStreamingButton").click(function(){
        startStreaming()
    });


});



function startStreaming () {


    var consumerKey = $("#ConsumerKeyText").val()
    var consumerSecret = $("#ConsumerSecretText").val()
    var accessToken = $("#AccessTokenText").val()
    var accessTokenSecret = $("#AccessTokenSecretText").val()

    console.log(""+ consumerKey + " " + consumerSecret + " " + accessToken + " "+ accessTokenSecret)

    $("#credentialslabel").text("")
    $("#resultslabel").text("")
    $("StartStreamingButton").prop("disabled",true);

    $.ajax({
        url: "startStreaming",
        type: "GET",
        data : { "consumerKey" : consumerKey,  "consumerSecret" : consumerSecret, "accessToken":accessToken, "accessTokenSecret":accessTokenSecret},
        contentType: "application/json",
        dataType: "json"
    }).success(function (dto) {

        if(dto.message) {
            if (dto.message != null) {
                $("#credentialslabel").text("Validated: " + dto.validated)

                $("#resultslabel").text("Message: " + dto.message)
            }else{
                console.log("startStreaming:" + JSON.stringify(dto))

            }
        }else{
            console.log("startStreaming:" + JSON.stringify(dto))
        }


    }).error(function(error){
        console.log(error)
    }).complete(function(){
        $("StartStreamingButton").prop("disabled", false);
    })
}

