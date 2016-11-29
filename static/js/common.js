function getActivityColor(activity) {
	    var pointColor = 'magenta';
	    switch (activity) {
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
		// A: Train
		case 'TRAIN':
		pointColor = '#f7f700';
		break;
        // B: Subway, Tram
		case 'SUBWAY':
		case 'TRAM':
		pointColor = '#f6bd00';
		break;
        // C: Bus, Ferry
		case 'FERRY':
		case 'BUS':
		pointColor = '#e66313';
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
	    return pointColor;
}
