$(document).ready(function() {
    var content = $('#content');

    function lazydraw(points, cw, ch, ctx) {
        var xmin = 999;
        var xmax = -999;
        var ymin = 999;
        var ymax = -999;
        for (var i = 0; i < points.length; ++i) {
            xmin = Math.min(xmin, points[i][0][1]);
            xmax = Math.max(xmax, points[i][0][1]);
            ymin = Math.min(ymin, points[i][0][2]);
            ymax = Math.max(ymax, points[i][0][2]);
        }
        var xcen = (xmin + xmax) / 2;
        var ycen = (ymin + ymax) / 2;
        var xext = xmax - xmin;
        var yext = ymax - ymin;
        var cosy = Math.cos(ycen * Math.PI / 180);
        var xzoo = Math.log(cw / xext / 256 * 360) / Math.log(2);
        var yzoo = Math.log(ch / yext / 256 * 360 * cosy) / Math.log(2);
        var zoom = Math.floor(Math.min(xzoo, yzoo));
        var google_tile = "http://maps.google.com/maps/api/staticmap" +
            "?style=feature:all|visibility:off" +
            "&style=feature:landscape|element:geometry|visibility:on" +
                "|color:0xffffff" +
            "&style=feature:water|visibility:on|color:0x000000" +
            "&style=feature:road|element:geometry|color:0x7f7f7f" +
                "|visibility:simplified" +
            "&style=feature:transit.line|visibility:on|color:0x7f7f7f" +
            "&center=" + ycen + "," + xcen + "&zoom=" + zoom +
            "&size=" + cw + "x" + ch + "&key=" + key;
        var imageObj = new Image();
        imageObj.src = google_tile;
        imageObj.onload = function(){
            ctx.globalAlpha = 0.1;
            ctx.drawImage(imageObj, 0, 0);
            lazydrawpoints(points, cw, ch, ctx, xcen, ycen, zoom, cosy);
        }
        imageObj.onerror = function(){ // draw points even if bg map fails
            lazydrawpoints(points, cw, ch, ctx, xcen, ycen, zoom, cosy);
        }
    }

    function lazydrawpoints(points, cw, ch, ctx, xcen, ycen, zoom, cosy) {
        var r = 2; // Math.pow(2, zoom) / 500;
        for (var i = 0; i < points.length; ++i) {
            var mode = points[i][0][0];
            var xdeg = points[i][0][1];
            var ydeg = points[i][0][2];
            var prob = points[i][1];
            var xpix = cw / 2 + (xdeg - xcen) * 256 * Math.pow(2, zoom) / 360;
            var ypix = ch / 2 - (ydeg - ycen) * 256 * Math.pow(2, zoom) / 360 / cosy;
            ctx.fillStyle = getActivityColor(mode);
            ctx.globalAlpha = prob;
            ctx.fillRect(xpix - r, ypix - r, 2*r, 2*r);
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
            h1.text(odgroup[0][0] + " \u2014 " + odgroup[0][1]);
            trips.append(h1);
            odgroup[1].forEach(function (route) {
                var canvas = canvas_route(route.probs, 200, 200);
                trips.append(canvas);
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
        var datelink = document.createElement("a");
        datelink.textContent = date;
        datelink.setAttribute("href", "/energymap#" + date);
        daycell.appendChild(datelink);
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
