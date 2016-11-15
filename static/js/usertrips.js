$(document).ready(function() {
    var content = $('#content');

    var date = $('#date');
    date.pickadate({
	clear: '',
	firstDay: 1,
	format: 'dd.mm.yyyy',
	hiddenName: true,
	onClose: function() {
	    location.hash = this.get('select', 'yyyy-mm-dd');
	    date.blur(); // https://github.com/amsul/pickadate.js/issues/160
	},
    });

    function update(date) {
        $("#date").pickadate("picker").set("select", Date.parse(date));

	content.css('opacity', 0.1);
	$.getJSON('trips_json?date=' + date, function(response) {
            var trip = $("#trip");
            trip.empty();
            if (response.length == 0)
                trip.html("<tr><td>No trips.</td></tr>");
            response.forEach(function (row) {
                var tr = $(document.createElement("tr"));
                trip.append(tr);
                row.forEach(function (col) {
                    var td = $(document.createElement("td"));
                    tr.append(td);
                    td.attr("colspan", col[1]);
                    td.text(col[0]);
                    var mode = col[0].split(" ")[0];
                    if (mode.match(/[A-Z]/)) { // activity cell, kinda rough...
                        td.attr("class", mode);
                        var glyph = null;
                        switch (mode) {
                        case 'ON_BICYCLE': glyph = "\uD83D\uDEB4\uFE0E"; break;
                        case 'WALKING':
                        case 'ON_FOOT':    glyph = "\uD83D\uDEB6\uFE0E"; break;
                        case 'RUNNING':    glyph = "\uD83C\uDFC3\uFE0E"; break;
                        case 'IN_VEHICLE': glyph = "\uD83D\uDE98\uFE0E"; break;
                        case "TRAIN":      glyph = "\uD83D\uDE82\uFE0E"; break;
                        case 'SUBWAY':     glyph = "\uD83D\uDE87\uFE0E"; break;
                        case 'TRAM':       glyph = "\uD83D\uDE8B\uFE0E"; break;
                        case 'FERRY':      glyph = "\u26F4\uFE0E"; break;
                        case 'BUS':        glyph = "\uD83D\uDE8D\uFE0E"; break;
//                      case 'BUS':        glyph = "\uD83D\uDE8C\uFE0E"; break;
                        case 'TILTING':    glyph = "/"; break;
                        case 'STILL':      glyph = "\xa0"; break;
//                      case 'STILL':      glyph = "\uD83D\uDECB\uFE0E"; break;
                        case 'UNKNOWN':    glyph = "?"; break;
//                      default:           glyph = "!"; break;
                        }
                        if (glyph) {
                            var icon = $(document.createElement("div"));
                            td.append(icon);
                            icon.text(glyph);
                        }
                    }
                });
            });
	    content.css('opacity', '');
	});
    }

    function hashchange() {
        var date = location.hash.split("#").splice(-1)[0];
        var today = (new Date()).toISOString().slice(0, 10);
        update(date || today);
    }
    $(window).on("hashchange", hashchange);
    hashchange();
});
