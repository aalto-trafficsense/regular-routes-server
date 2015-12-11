$(document).ready(function() {
	$('#signout').click(disconnectServer);
    var date = $('#date');
    date.pickadate({
	clear: '',
	firstDay: 1,
	format: 'dd.mm.yyyy',
	hiddenName: true,
	onClose: function() {
	    update(this.get('select', 'yyyy-mm-dd'));
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
	if (type === 'raw-point') {
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
	if (date === currentDate)
	    return;
	currentDate = date;
	mapCanvas.css('opacity', 0.1);
	map.data.forEach(function(feature) {
	    map.data.remove(feature);
	});
	$.getJSON('../energy/geojson?date=' + date, function(response) {
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

	function disconnectServer() {
      // Revoke the server tokens
      $.ajax({
        type: 'POST',
        url: $(location).attr('origin') + '/disconnect',
        async: false,
        success: function(result) {
            console.log('revoke response: ' + result);
			window.location='signedout';
        },
        error: function(e) {
          console.log(e);
        }
      });
	}
});
