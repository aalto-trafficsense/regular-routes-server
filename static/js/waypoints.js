$(document).ready(function() {
    var mapCanvas = $('#map-canvas');
    var mapOptions = {
	center: { lat: 60.1841396, lng: 24.8300838 },
	zoom: 12
    };
    var map = new google.maps.Map(mapCanvas[0], mapOptions);
    map.data.setStyle(function(feature) {
	var type = feature.getProperty('type');
	var title = feature.getProperty('title');
        if (type === 'route-point') {
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
	}
        else if (type === 'route-point-cluster') {
	    return {
		icon: {
		    path: google.maps.SymbolPath.BACKWARD_CLOSED_ARROW,
		    scale: 20,
		    fillColor: 'gold',
		    fillOpacity: 0.2,
		    strokeColor: 'black',
			strokeWeight: 1
		},
		title: title
	    };
	}
    });

    map.data.addListener('click', function(event) {
        map.panTo(event.feature.getGeometry().get());
        map.setZoom(map.getZoom() + 1);
    });

    google.maps.event.addListener(map, 'idle', function() {
        var bounds = map.getBounds();
        $.getJSON(
            'waypoints/geojson',
            { bounds: JSON.stringify(bounds.toJSON()) },
            function(response) {
                map.data.forEach(function(feature) {
                    map.data.remove(feature);
                });
	        map.data.addGeoJson(response);
        });
    });
});
