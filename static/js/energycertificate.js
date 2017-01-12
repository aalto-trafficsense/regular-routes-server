$(document).ready(function() {

    var firstpicker = $('#firstday');
    firstpicker.pickadate({
	firstDay: 1,
	format: 'dd.mm.yyyy',
	hiddenName: true,
	onClose: function() {
            var dates = location.hash.split("#").splice(-1)[0].split("/");
            dates[0] = this.get('select', 'yyyy-mm-dd');
            dates[1] = dates[1] || "";
            location.hash = dates.join("") && dates.join("/") || "";
	    firstpicker.blur();
	},
    });

    var lastpicker = $('#lastday');
    lastpicker.pickadate({
	firstDay: 1,
	format: 'dd.mm.yyyy',
	hiddenName: true,
	onClose: function() {
            var dates = location.hash.split("#").splice(-1)[0].split("/");
            dates[0] = dates[0] || "";
            dates[1] = this.get('select', 'yyyy-mm-dd');
            location.hash = dates.join("") && dates.join("/") || "";
	    lastpicker.blur();
	},
    });

    function update(firstday, lastday) {
        if (firstday)
            $("#firstday").pickadate("picker").set("select", Date.parse(firstday));
        else
            $("#firstday").pickadate("picker").clear();

        if(lastday)
            $("#lastday").pickadate("picker").set("select", Date.parse(lastday));
        else
            $("#lastday").pickadate("picker").clear();

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

    function hashchange() {
        var dates = location.hash.split("#").splice(-1)[0].split("/");
        update(dates[0], dates[1]);
    }
    $(window).on("hashchange", hashchange);
    hashchange();

});
