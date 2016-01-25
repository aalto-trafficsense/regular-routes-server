$(document).ready(function() {
    var mapCanvas = $('#map-canvas');
    var mapOptions = {
	center: { lat: 60.1841396, lng: 24.8300838 },
	zoom: 12
    };
    var map = new google.maps.Map(mapCanvas[0], mapOptions);
    map.data.setStyle(function(feature) {
	var type = feature.getProperty('type');
	var title = feature.getProperty('predtype');
	if (type === 'Prediction') {
	    var pointColor = 'magenta';
	    switch(feature.getProperty('activity')) {
	    case 'UNSPECIFIED':
		pointColor = 'yellow';
		break;
	    default:
		pointColor = 'black';
	    } // end-of-switch
	    return {
		icon: {
		    path: google.maps.SymbolPath.BACKWARD_CLOSED_ARROW,
		    scale: 3,
		    fillColor: pointColor,
		    fillOpacity: 1.0,
		    strokeColor: 'blue'
		},
		title: title
	    };
	} else if (type === 'something_else') {
	    return {
		icon: {
		    path: google.maps.SymbolPath.FORWARD_CLOSED_ARROW,
		    scale: 3,
		    fillColor: 'yellow',
		    fillOpacity: 1.0,
		    strokeColor: 'black'
		},
		title: title
	    };
	}
    });

	$.getJSON('../predict/' + device_id, function(response) {
	    map.data.addGeoJson(response);
		if (response.features.length == 1) {
			var coords = feature.geometry.coordinates;
		    map.panTo(new google.maps.LatLng(coords[1],coords[0]));
			map.setZoom(5);
		}
	});
    });
