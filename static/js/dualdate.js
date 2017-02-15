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

    function hashchange() {
        var hash = location.hash.split("#").splice(-1)[0];
        var dates = hash.split("/");
        var firstday = dates[0];
        var lastday = dates[1];

        if (firstday)
            $("#firstday").pickadate("picker").set(
                "select", Date.parse(firstday));
        else
            $("#firstday").pickadate("picker").clear();

        if (lastday)
            $("#lastday").pickadate("picker").set(
                "select", Date.parse(lastday));
        else
            $("#lastday").pickadate("picker").clear();

        // toISOString formats conveniently only in UTC, fudge zone offset in
        var last = new Date();
        last.setMinutes(last.getMinutes() - last.getTimezoneOffset());

        // Depending on view, presets may want to end on today or yesterday
        var offset = $("#presets").attr("offset") || 0;
        last.setDate(last.getDate() + parseInt(offset));
        lastStr = last.toISOString().slice(0, 10);

        var rweek = new Date(last.getTime());
        rweek.setDate(rweek.getDate() - 6);
        rweek = rweek.toISOString().slice(0, 10) + "/" + lastStr;

        var rmonth = new Date(last.getTime());
        rmonth.setMonth(rmonth.getMonth() - 1);
        rmonth.setDate(rmonth.getDate() + 1);
        rmonth = rmonth.toISOString().slice(0, 10) + "/" + lastStr;

        var ryear = new Date(last.getTime());
        ryear.setFullYear(ryear.getFullYear() - 1);
        ryear.setDate(ryear.getDate() + 1);
        ryear = ryear.toISOString().slice(0, 10) + "/" + lastStr;

        var presets = [
            {id: "#weeklink", href: rweek},
            {id: "#monthlink", href: rmonth},
            {id: "#yearlink", href: ryear}];

        for (var i = 0; i < presets.length; ++i)
            if (hash == presets[i].href)
                $(presets[i].id).removeAttr("href")
            else
                $(presets[i].id).attr("href", "#" + presets[i].href);
    }

    $(window).on("hashchange", hashchange);
    hashchange();
});
