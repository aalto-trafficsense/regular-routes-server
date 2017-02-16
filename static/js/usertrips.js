$(document).ready(function() {
    var content = $('#content');

    function eat(response) {
            var trips = $("#trip");
            trips.empty();
            if (response.length == 0)
                trips.html("<table><tr><td>No trips.</td></tr></table>");
            response.forEach(function (day) {
                var daydate = day["date"];
                var daydata = day["data"];
                var daytable = buildday(daydata, daydate);
                trips.append(daytable);
            });
	    content.css('opacity', '');
    }

    function buildday(data, date) {
        var trip = $(document.createElement("table"));

        var daycell = document.createElement("td");
        daycell.appendChild(document.createTextNode(date));
        var p = daycell.appendChild(document.createElement("p"));
        var weekday = (new Date(Date.parse(date))).toDateString().slice(0, 3);
        p.appendChild(document.createTextNode(weekday));
        daycell.setAttribute("rowspan", 3);
        daycell.setAttribute("class", "daycell");

        $.each(data, function (i, row) {
                var classname = row[0];
                var cells = row[1];
                var tr = $(document.createElement("tr"));
                trip.append(tr);
                if (daycell) {
                    tr.append(daycell)
                    daycell = null;
                }
                cells.forEach(function (col) {
                    var td = $(document.createElement("td"));
                    td.addClass(classname);
                    tr.append(td);
                    td.attr("colspan", col[1]);
                    if (col[0] === null) {
                        td.addClass("gap");
                    } else if (classname == "time") {
                        td.text(col[0][0]);
                        switch (col[0][1]) { // align
                        case "start": td.addClass("left"); break;
                        case "both": td.addClass("left both"); break;
                        case "end": td.addClass("right"); break;
                        }
                    } else if (classname == "activity") {
                        var activity = col[0][0];
                        td.text(activity);
                        var mode = activity.split(" ")[0];
                        td.addClass(mode);
                        var glyph = null;
                        switch (mode) {
                        case 'ON_BICYCLE': glyph = "directions_bike"; break;
                        case 'WALKING':
                        case 'ON_FOOT':    glyph = "directions_walk"; break;
                        case 'RUNNING':    glyph = "directions_run"; break;
                        case 'IN_VEHICLE': glyph = "directions_car"; break;
                        case "TRAIN":      glyph = "train"; break;
                        case 'SUBWAY':     glyph = "subway"; break;
                        case 'TRAM':       glyph = "tram"; break;
                        case 'FERRY':      glyph = "directions_boat"; break;
                        case 'BUS':        glyph = "directions_bus"; break;
                        case 'TILTING':    glyph = "screen_rotation"; break;
                        case 'STILL':      glyph = "\xa0"; break;
                        case 'UNKNOWN':    glyph = "?"; break;
//                      default:           glyph = "!"; break;
                        }

                        var icon = $(document.createElement("div"));
                        var matic = $(document.createElement("span"));
                        matic.addClass("material-icons");
                        matic.text(glyph);
                        icon.append(matic);
                        td.append(icon);

                        td.append(col[0][1]); // duration

                    } else if (classname == "place") {
                        var label = col[0];
                        if (label === false) {
                            var div = $(document.createElement("div"));
                            td.append(div);
                            $(td).addClass("move");
                            return; // uh
                        }

                        // Disallow slash alone on a line, bind to shorter word
                        var names = label.split(" / ");
                        var text = names[0];
                        if (names.length > 1) {
                            var lof = names[0].split(" ").slice(-1)[0];
                            var fol = names[1].split(" ")[0];
                            var sep = lof.length < fol.length
                                ? "\xa0/ " : " /\xa0";
                            // Join tail just in case there were more slashes,
                            // js has maxsplit would crop the tail
                            text += sep + names.slice(1).join(" / ");
                        }
                        $(td).text(text);
                    }
                });
        });
        return trip;
    }

    function hashchange() {
        var hash = location.hash.split("#").splice(-1)[0];
        var dates = hash.split("/");
        var firstday = dates[0];
        var lastday = dates[1];

        var args = [];
        if (firstday)
            args.push("firstday=" + firstday);
        if (lastday)
            args.push("lastday=" + lastday);
        args = args.join("&");
        var qs = args ? "?" + args : "";

	content.css('opacity', 0.1);
	$.getJSON("trips_json" + qs, eat);

        $("#csvlink").attr("href", "trips_csv" + qs);
    }

    $(window).on("hashchange", hashchange);
    hashchange();
});
