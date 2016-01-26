$(document).ready(function() {
    var mapCanvas = $('#map-canvas');
    var mapOptions = {
	center: { lat: 60.1841396, lng: 24.8300838 },
	zoom: 12
    };
	mapCanvas.css('opacity', 0.1);
    var map = new google.maps.Map(mapCanvas[0], mapOptions);
    map.data.setStyle(function(feature) {
	var type = feature.getProperty('type');
	var title = feature.getProperty('title');
	if (type === 'Prediction') {
	    var pointColor = 'red';
		var pointScale = 1;
	    switch(feature.getProperty('minutes')) {
	    case 1:
			pointColor = 'white';
			pointScale = 3;
			break;
	    case 5:
			pointColor = 'magenta';
			pointScale = 5;
			break;
	    case 30:
			pointColor = 'blue';
			pointScale = 7;
			break;
	    default:
			pointColor = 'black';
	    } // end-of-switch
	    return {
		icon: {
		    path: google.maps.SymbolPath.CIRCLE,
		    scale: pointScale,
			fillColor: pointColor,
			fillOpacity: 0.3,
		    strokeColor: black,
		    strokeOpacity: 1.0,
			strokeWeight: 1
		},
		title: title
	    };
	} else if (type === 'something_else') {
	    return {
		icon: {
		    path: google.maps.SymbolPath.BACKWARD_CLOSED_ARROW,
		    scale: 3,
		    fillColor: 'LightRed',
		    fillOpacity: 1.0,
		    strokeColor: 'Red',
			strokeWeight: 1
		},
		title: title
	    };
	}
    });

	$.getJSON('../predictgeojson/' + device_id, function(response) {
	    map.data.addGeoJson(response);
		if (response.features.length > 1) {
			var bounds = new google.maps.LatLngBounds();
			response.features.forEach(function (feature) {
				var coords = feature.geometry.coordinates;
			    bounds.extend(new google.maps.LatLng(coords[1],coords[0]));
			});
			map.fitBounds(bounds);
			mapCanvas.css('opacity', '');
		}
	});
    });
