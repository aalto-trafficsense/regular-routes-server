$(document).ready(function() {
    var content = $('#content');

    function lazydraw(points, cw, ch, ctx) {
        var modes = [];
        var xs = [];
        var ys = [];
        var probs = [];
        var ymin = 999;
        var ymax = -999;
        for (var i = 0; i < points.length; ++i) {
            // Fish approximate coords from the ID, or could query for exact
            // var lola = points[i][0][1];
            // var lat = - lola%1000000/10000; // y is down on canvas
            // var lon = Math.floor(lola/1000000)/10000;
            var lon = points[i][0][1];
            var lat = -points[i][0][2]; // y is down on canvas
            console.log(lat, lon);
            ymin = Math.min(ymin, lat);
            ymax = Math.max(ymax, lat);

            modes.push(points[i][0][0]);
            xs.push(lon);
            ys.push(lat);
            probs.push(points[i][1]);
        }
        var xmin = 999;
        var xmax = -999;
        var xscale = Math.cos(Math.PI / 180 * ((ymin + ymax) / 2));
        for (var i = 0; i < points.length; ++i) {
            // Rough equirectangular projection
            xs[i] *= xscale;
            xmin = Math.min(xmin, xs[i]);
            xmax = Math.max(xmax, xs[i]);
        }
        var r = 2;
        var scale = Math.min((cw-2*r) / (xmax-xmin), (ch-2*r) / (ymax-ymin));
        var xoff = (cw - scale * (xmax - xmin)) / 2;
        var yoff = (ch - scale * (ymax - ymin)) / 2;
        for (var i = 0; i < points.length; ++i) {
            xs[i] = scale * (xs[i] - xmin) + xoff;
            ys[i] = scale * (ys[i] - ymin) + yoff;
        }
        for (var i = 0; i < points.length; ++i) {
            ctx.fillStyle = getActivityColor(modes[i]);
            ctx.globalAlpha = probs[i];
            ctx.fillRect(xs[i] - r, ys[i] - r, 2*r, 2*r);
        }
    }

    function canvas_route(points, cw, ch) {
        var canvas = $(document.createElement("canvas"));
        canvas.attr("width", cw);
        canvas.attr("height", ch);
        var ctx = canvas.get(0).getContext("2d");
        setTimeout(lazydraw, 0, points, cw, ch, ctx);
        return canvas;
    }

    function eat(response) {
        var trips = $("#trip");
        trips.empty();
        if (response.length == 0)
            trips.html("<table><tr><td>No trips.</td></tr></table>");
        response.clustered.forEach(function (odgroup) {
            var h1 = $(document.createElement("h1"));
            h1.text(odgroup[0]);
            trips.append(h1);
            odgroup[1].forEach(function (route) {
//                var h2 = $(document.createElement("h2"));
//                h2.text(route.probs.join(" "));
//                trips.append(h2);

                var canvas = canvas_route(route.probs, 200, 200);
                trips.append(canvas);
                // $(document.createElement("canvas"));
                // canvas.attr("width", 100);
                // canvas.attr("height", 100);
                // trips.append(canvas);
                // var ctx = canvas.get(0).getContext("2d");
                // ctx.fillStyle = 'rgb(200, 0, 0)';
                // ctx.fillRect(10, 10, 50, 50);

                route.trips.forEach(function (trip) {
                    var tripdata = response.trips[trip["id"]];
                    var daydate = tripdata.date;
                    var daydata = tripdata.render;
                    var daytable = buildday(daydata, daydate);
                    trips.append(daytable);
                });
            });
        });
	content.css('opacity', '');
    }

    function buildday(data, date) {
        var trip = $(document.createElement("table"));
        setTimeout(lazyfill, 0, data, date, trip);
        return trip;
    }

    function lazyfill(data, date, trip) {
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
                        td.append(document.createElement("br"));
                        td.append(activity);
                        td.append(document.createElement("br"));
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
	$.getJSON("routes_json" + qs, eat);

        $("#csvlink").attr("href", "routes_csv" + qs);
    }

    $(window).on("hashchange", hashchange);
    hashchange();
});
