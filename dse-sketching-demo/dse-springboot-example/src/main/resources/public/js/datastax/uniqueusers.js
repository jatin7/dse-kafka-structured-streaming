/**
 * Created by michaelraney on 3/16/18.
 */




function pollUniqueUsers(start) {

    setTimeout(function () {

        $.ajax({
            url: "getUniqueUsersForToday",
            type: "get",
            contentType: "application/json",
            dataType: "json"
        }).success(function (dto) {


            if(dto.approximates) {
                if (dto.approximates != null) {
                    if (dto.approximates.length > 0) {
                        refreshUniqueUserTable(dto.approximates);
                    }
                }else{
                    console.log("getUniqueUsersForToday:" + JSON.stringify(dto))
                }
            }else{
                console.log("getUniqueUsersForToday:" + JSON.stringify(dto))
            }

            $("#uniqueUserQueryTime").text("Response Time: "+dto.resultTime + " ms");

        }).complete(function (dto) {
            // Recursive Poll
            pollUniqueUsers();// Call poll after complete
        });
    }, 11000);//eleven seconds
}

function refreshUniqueUserTable(data) {

    //data = [{'time':new Date(), 'uniqueUsers':10}] yyyy-mm-dd HH:mm:ss
    data = MG.convert.date(data, 'time', '%Y-%m-%d %H:%M:%S%Z');
    //console.log("Result=" + JSON.stringify(data));

    MG.data_graphic({
        title: "Unique Users",
        description: "Each point represents unique users who have tweeted within 10 second interval",
        data: data,
        width:850,
        height: 200,
        buffer:15,
        top:50,
        left:100,
        target: '#mygraph',
        x_accessor: 'time',
        y_accessor: 'uniqueUsers',
        x_label: 'Time',
        y_label: 'Unique Users'


    })


}
function pollUniqueUsersRollup(start) {

    setTimeout(function () {

        $.ajax({
            url: "getUniqueUsersRollup",
            type: "get",
            contentType: "application/json",
            dataType: "json"
        }).success(function (dto) {


            // Refresh UI Data
            if(dto.approximates) {
                if (dto.approximates != null) {
                    if (dto.approximates.length > 0) {
                        refreshUniqueUserTableRollup(dto.approximates);
                    }
                }else{
                    console.log("getUniqueUsersRollup:" + JSON.stringify(dto))
                }
            }else{
                console.log("getUniqueUsersRollup:" + JSON.stringify(dto))
            }


            $("#uniqueUserRollupQueryTime").text("Response Time: "+dto.resultTime + " ms");


            // refreshAccountTable(dto);
            // refreshDetailsTable(dto);
        }).complete(function (dto) {
            // Recursive Poll
            pollUniqueUsersRollup();// Call poll after complete
        });
    }, 20000);//twenty seconds
}

function refreshUniqueUserTableRollup(data) {

    //data = [{'time':new Date(), 'uniqueUsers':10}]
    data = MG.convert.date(data, 'time', '%Y-%m-%d %H:%M:%S%Z');
    //console.log("Result=" + JSON.stringify(data));

    MG.data_graphic({
        title: "Unique Users 10min Aggregate",
        description: "Each point represents unique users who have tweeted within 10 minute interval",
        data: data,
        width: 500,
        height: 200,
        buffer:15,
        top:50,
        left:100,
        target: '#mygraphrollup',
        x_accessor: 'time',
        y_accessor: 'uniqueUsers',
        x_label: 'Time',
        y_label: 'Unique Users'

    })

}
function initializeUniqueUserGraphBeforePolling(){

    $.ajax({
        url: "getUniqueUsersForToday",
        type: "get",
        contentType: "application/json",
        dataType: "json"
    }).success(function (dto) {


        if(dto.approximates) {
            if (dto.approximates != null) {
                if (dto.approximates.length > 0) {
                    refreshUniqueUserTable(dto.approximates);
                }
            }else{
                console.log("getUniqueUsersForToday:" + JSON.stringify(dto))
            }
        }else{
            console.log("getUniqueUsersForToday:" + JSON.stringify(dto))
        }

        $("#uniqueUserQueryTime").text("Response Time: "+dto.resultTime + " ms");

    })

    $.ajax({
        url: "getUniqueUsersRollup",
        type: "get",
        contentType: "application/json",
        dataType: "json"
    }).success(function (dto) {


        // Refresh UI Data
        if(dto.approximates) {
            if (dto.approximates != null) {
                if (dto.approximates.length > 0) {
                    refreshUniqueUserTableRollup(dto.approximates);
                }
            }else{
                console.log("getUniqueUsersRollup:" + JSON.stringify(dto))
            }
        }else{
            console.log("getUniqueUsersRollup:" + JSON.stringify(dto))
        }


        $("#uniqueUserRollupQueryTime").text("Response Time: "+dto.resultTime + " ms");


        // refreshAccountTable(dto);
        // refreshDetailsTable(dto);
    });
}