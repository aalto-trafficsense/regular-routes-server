$(document).ready(function() {

    function hashchange() {
        var hash = location.hash.split("#").splice(-1)[0];
        var dates = hash.split("/");
        var firstday = dates[0];
        var lastday = dates[1];

        var url = "energycertificate_svg";
        var args = [];
        if (firstday)
            args.push("firstday=" + firstday);
        if (lastday)
            args.push("lastday=" + lastday);
        args = args.join("&");
        if (args)
            url += "?" + args;
        $("#svg").attr("src", url);
    }

    $(window).on("hashchange", hashchange);
    hashchange();

});
