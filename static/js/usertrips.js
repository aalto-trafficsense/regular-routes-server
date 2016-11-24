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
	                case 'ON_BICYCLE': glyph = "\uD83D\uDEB4"; break;
	                case 'WALKING':
	                case 'ON_FOOT':    glyph = "\uD83D\uDEB6"; break;
	                case 'RUNNING':    glyph = "\uD83C\uDFC3"; break;
	                case 'IN_VEHICLE': glyph = "\uD83D\uDE98"; break;
                        case "TRAIN":      glyph = "\uD83D\uDE82"; break;
	                case 'SUBWAY':     glyph = "\uD83D\uDE87"; break;
	                case 'TRAM':       glyph = "\uD83D\uDE8B"; break;
	                case 'FERRY':      glyph = "\u26F4"; break;
	                case 'BUS':        glyph = "\uD83D\uDE8D"; break;
//	                case 'BUS':        glyph = "\uD83D\uDE8C"; break;
	                case 'TILTING':    glyph = "/"; break;
//	                case 'STILL':      glyph = "\uD83D\uDECB"; break;
	                case 'UNKNOWN':    glyph = "?"; break;
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
