/**
 * Created by michaelraney on 3/16/18.
 */



function initializeTopTweetsTable () {

    var tagstable = $('#toptagstable').DataTable({
        "searching": false,
        "lengthChange": false,
        "pageLength": 30,
        "scrollY": "200px",
        "scrollCollapse": true,
        "paging": false,
        "info": false,


        "columns": [
            {
                title: 'Hashtag', name: 'Hashtag', data: 'Hashtag', type: 'numeric',
                fnCreatedCell: function (nTd, sData, oData, iRow, iCol) {
                    $(nTd).html("<a href='https://twitter.com/search?q=%23" + oData.Hashtag + "' target='_blank'>" + oData.Hashtag + "</a>");
                }
            },
            {title: 'Occurrences', name: 'Occurrences', data: 'Occurrences'}
        ],


        "order": [[1, "desc"]]
    })

    tagstable.order([1, 'desc']).draw()

    $.ajax({
        url: "getTopTweetsRollup",
        type: "get",
        contentType: "application/json",
        dataType: "json"
    }).success(function (dto) {




        //console.log("Result=" + JSON.stringify(dto));

        if(dto.topTags) {
            if (dto.topTags != null) {
                formatTableResult(dto.topTags);

            }else{
                console.log("getTopTweetsRollup:" + JSON.stringify(dto))
            }
        }else{
            console.log("getTopTweetsRollup:" + JSON.stringify(dto))
        }
        $("#topTweetsQueryTime").text("Response Time: "+dto.resultTime + " ms");

        // Refresh UI Data


        // refreshAccountTable(dto);
        // refreshDetailsTable(dto);
    }).complete(function (dto) {
        // Recursive Poll
        getTopTweetsRollup();// Call poll after complete
    });

}

function getTopTweetsRollup(start) {

    setTimeout(function () {

        $.ajax({
            url: "getTopTweetsRollup",
            type: "get",
            contentType: "application/json",
            dataType: "json"
        }).success(function (dto) {




            //console.log("Result=" + JSON.stringify(dto));

            if(dto.topTags) {
                if (dto.topTags != null) {
                    formatTableResult(dto.topTags);

                }else{
                    console.log("getTopTweetsRollup:" + JSON.stringify(dto))
                }
            }else{
                console.log("getTopTweetsRollup:" + JSON.stringify(dto))
            }
            $("#topTweetsQueryTime").text("Response Time: "+dto.resultTime + " ms");

            // Refresh UI Data


            // refreshAccountTable(dto);
            // refreshDetailsTable(dto);
        }).complete(function (dto) {
            // Recursive Poll
            getTopTweetsRollup();// Call poll after complete
        });
    }, 20000);//twenty seconds
}
function formatTableResult(topTags){

    //$("#toptagsdiv").empty();

    // $('#toptagsdiv').append("<table id=\"toptagstable\"></table>" );

    var tagstable = $('#toptagstable').DataTable()

    tagstable.rows().remove().draw();

    $.each(topTags, function(key, value) {
        tagstable.row.add(
            { "Hashtag":key, "Occurrences":value}
        ).draw(false)
        tagstable.order( [1, 'desc'] ).draw()
    });

    tagstable.columns( 'Occurrences' )
        .order( 'desc' )
        .draw();


}

