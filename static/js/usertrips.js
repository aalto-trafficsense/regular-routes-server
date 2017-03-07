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

        $(".activity").not(".STILL").css("cursor", "pointer");
        $(".activity").not(".STILL").on("click", function(event) {
            var td = event.currentTarget;
            selectedLegId = td.getAttribute("legid");

            // Set mode on dialog
            var mode = td.className.split(" ")[1];
            $("#"+mode).prop("checked", true);
            $("#mode").buttonset("refresh");

            // Set line on dialog
            var parts = td.firstChild.textContent.split(" ");
            if (parts.length > 1)
                $("#line").val(parts[1]);

            dialog.dialog("open");
        });

	content.css('opacity', '');
    }

    function buildday(data, date) {
        var trip = $(document.createElement("table"));

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
                        td.text(activity);
                        var mode = activity.split(" ")[0];
                        td.addClass(mode);
                        var glyph = null;
                        switch (mode) {
                        case 'ON_BICYCLE': glyph = "\uE52F"; break;
                        case 'WALKING':
                        case 'ON_FOOT':    glyph = "\uE536"; break;
                        case 'RUNNING':    glyph = "\uE566"; break;
                        case 'IN_VEHICLE': glyph = "\uE531"; break;
                        case "TRAIN":      glyph = "\uE570"; break;
                        case 'SUBWAY':     glyph = "\uE56F"; break;
                        case 'TRAM':       glyph = "\uE571"; break;
                        case 'FERRY':      glyph = "\uE532"; break;
                        case 'BUS':        glyph = "\uE530"; break;
                        case 'TILTING':    glyph = "\uE1C1"; break;
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

                        td.attr("legid", col[0][2]);
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

    var selectedLegId;

    function setLegMode() {
        var checked = $.grep(
            $("#mode").get(0).children,
            function (x) {return (x.checked)})[0];
        var mode = $(checked).val();

        if (mode == "RESET")
            mode = null;

        var data = {
            id: selectedLegId, activity: mode, line_name: $("#line").val()};
        $.ajax({
            type: "POST",
            url: '/setlegmode',
            data: JSON.stringify(data),
            contentType: "application/json",
            success: hashforce});
        dialog.dialog("close");
    }

    function hashforce() {
        window.dispatchEvent(new HashChangeEvent("hashchange"));
    }

    function radioChecked(buttonset) {
        var checked = $.grep(
            buttonset.children,
            function (x) {return (x.checked)})[0];
        var mode = $(checked).val();
        var masses = ["FERRY", "SUBWAY", "TRAIN", "TRAM", "BUS"];
        if (-1 == $.inArray(mode, masses)) {
            $("#line").hide(200);
            $("#line").prev().hide(200);
        } else {
            $("#line").show(200);
            $("#line").prev().show(200);
        }
    }

    var dialog = $("#dialog-form").dialog({
        autoOpen: false,
        height: 400,
        width: 400,
        modal: true,
        buttons: {
            "Submit": setLegMode,
            Cancel: function() { dialog.dialog("close") }
        },
        close: function() {
            form[0].reset();
        },
        open: function() {
            $("#mode").buttonset().change(function (event) {
                radioChecked(event.currentTarget);
            });
            radioChecked($("#mode").get(0));
        }
    });

    var form = dialog.find("form").on("submit", function(event) {
        event.preventDefault();
        setLegMode();
    })

    $("#mode").buttonset();

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
        $("#csvlink").attr("href", "trips_csv" + qs);

	content.css('opacity', 0.1);
	$.getJSON("trips_json" + qs, eat);

    }

    $(window).on("hashchange", hashchange);
    hashchange();
});
