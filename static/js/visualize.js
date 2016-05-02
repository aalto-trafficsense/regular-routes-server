$(document).ready(function() {
    var date = $('#date');
    date.pickadate({
	clear: '',
	firstDay: 1,
	format: 'dd.mm.yyyy',
	hiddenName: true,
	onClose: function() {
	    update(this.get('select', 'yyyy-mm-dd'));

	    // don't pop up calendar when document refocused
	    // https://github.com/amsul/pickadate.js/issues/160
	    date.blur();
	}
    });
    date.pickadate('picker').open();

    var mapCanvas = $('#map-canvas');
    var mapOptions = {
	center: { lat: 60.1841396, lng: 24.8300838 },
	zoom: 12
    };
    var map = new google.maps.Map(mapCanvas[0], mapOptions);
    map.data.setStyle(function(feature) {
	var type = feature.getProperty('type');
	var title = feature.getProperty('title');
	var pointColor = 'magenta';
	switch(feature.getProperty('activity')) {
	    case 'ON_BICYCLE':
		pointColor = '#008c58';
		break;
	    case 'WALKING':
	    case 'ON_FOOT':
		pointColor = '#20ac29';
		break;
	    case 'RUNNING':
		pointColor = '#add500';
		break;
	    case 'IN_VEHICLE':
		pointColor = '#dd0020';
		break;
	    case 'TILTING':
		pointColor = 'blue';
		break;
	    case 'STILL':
		pointColor = 'white';
		break;
	    case 'UNKNOWN':
		pointColor = 'gray';
		break;
	    default:
		pointColor = 'black';
	} // end-of-switch
	if (type === 'raw-point') {
	    return {
		icon: {
		    path: google.maps.SymbolPath.CIRCLE,
		    scale: 3,
		    strokeColor: pointColor,
		    strokeOpacity: 0.5
		},
		title: title
	    };
	} else if (type === 'route-point') {
	    return {
		icon: {
		    path: google.maps.SymbolPath.BACKWARD_CLOSED_ARROW,
		    scale: 3,
		    fillColor: 'gold',
		    fillOpacity: 1.0,
		    strokeColor: 'black',
			strokeWeight: 1
		},
		title: title
	    };
	} else if (type === 'trace-line') {
	    return {
		strokeColor: pointColor,

		// white is rather invisible at low opacity
		strokeOpacity: (pointColor == 'white' && .8 || .2)
	    };
	}
    });

    var currentDate;

    function update(date) {
	if (date === currentDate)
	    return;
	currentDate = date;
	mapCanvas.css('opacity', 0.1);
	map.data.forEach(function(feature) {
	    map.data.remove(feature);
	});
	$.getJSON('../visualize/' + device_id + '/geojson?date=' + date, function(response) {
	    map.data.addGeoJson(response);
	    mapCanvas.css('opacity', '');
		if (response.features.length > 1) {
			var bounds = new google.maps.LatLngBounds();
			response.features.forEach(function (feature) {
			    // coordinates may be a pair or a list of pairs
			    var coordlist = feature.geometry.coordinates;
			    if (! (coordlist[0] instanceof Array))
				coordlist = [coordlist];
			    coordlist.forEach(function (coords) {
				bounds.extend(new google.maps.LatLng(
				    coords[1], coords[0]));
			    });
			});
			map.fitBounds(bounds);
		}
	});
    }
});
