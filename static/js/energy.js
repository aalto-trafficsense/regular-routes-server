$(document).ready(function() {
	$('#menu').click(function() { window.location='menu' } );
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

    var mapCanvas = $('#map-canvas');
    var mapOptions = {
	center: { lat: 60.1841396, lng: 24.8300838 },
	zoom: 12
    };
    var map = new google.maps.Map(mapCanvas[0], mapOptions);
    map.data.setStyle(function(feature) {
	var type = feature.getProperty('type');
	var title = feature.getProperty('title');
	if (type === 'raw-point') {
	    var pointColor = getActivityColor(feature.getProperty('activity'));
	    return {
		icon: {
		    path: google.maps.SymbolPath.CIRCLE,
		    scale: 3,
		    strokeColor: pointColor,
		    strokeOpacity: 0.5
		},
		title: title
	    };
	}
    });

    var currentDate;

    function update(date) {
        $("#date").pickadate("picker").set("select", Date.parse(date));

	if (date === currentDate)
	    return;
	currentDate = date;
	mapCanvas.css('opacity', 0.1);
	map.data.forEach(function(feature) {
	    map.data.remove(feature);
	});
	$.getJSON('..' + RR_URL_PREFIX + '/energymap/geojson?date=' + date, function(response) {
	    map.data.addGeoJson(response);
	    mapCanvas.css('opacity', '');
		if (response.features.length > 1) {
			var bounds = new google.maps.LatLngBounds();
			response.features.forEach(function (feature) {
				var coords = feature.geometry.coordinates;
		    	bounds.extend(new google.maps.LatLng(coords[1],coords[0]));
			});
			map.fitBounds(bounds);
		}
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
