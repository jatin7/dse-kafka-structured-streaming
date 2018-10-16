/**
 * Created by michaelraney on 3/16/18.
 */


$(document).ready(function(){

    initializeUniqueUserGraphBeforePolling();
    initializeTopTweetsTable()

    pollUniqueUsers()
    pollUniqueUsersRollup()
    getTopTweetsRollup()
});