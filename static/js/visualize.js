$(window).on('load', function() {
  var mapCanvas = $('#map-canvas');
  var mapOptions = {
    center: { lat: 60.1841396, lng: 24.8300838 },
    zoom: 16
  };
  var map = new google.maps.Map(mapCanvas[0], mapOptions);
  map.data.setStyle(function(feature) {
    var type = feature.getProperty('type');
    var title = feature.getProperty('title');
    if (type === 'raw-point') {
      return {
        icon: {
          path: google.maps.SymbolPath.CIRCLE,
          scale: 3,
          strokeColor: '#ff00ff',
          strokeOpacity: 0.5
        },
        title: title
      }
    } else if (type === 'snap-line') {
      return {
        strokeColor: 'blue',
        strokeOpacity: 0.5
      };
    } else if (type === 'route-line') {
      return {
        strokeColor: 'red',
      }
    } else if (type === 'route-point') {
      return {
        icon: {
          path: google.maps.SymbolPath.CIRCLE,
          scale: 3,
          strokeColor: 'red',
        },
        title: title
      }
    } else if (type === 'link-line') {
      return {
        strokeColor: 'green',
        strokeOpacity: 0.5
      }
    } else if (type === 'link-point') {
      return {
        icon: {
          path: google.maps.SymbolPath.CIRCLE,
          scale: 2,
          strokeColor: 'green',
          strokeOpacity: 0.5,
        },
        title: title
      };
    }
  });

  function update() {
    mapCanvas.css('opacity', 0.1);
    $.getJSON('../visualize/' + device_id + '/geojson', function(response) {
      map.data.addGeoJson(response);
      mapCanvas.css('opacity', '');
    });
  }

  update();
});
