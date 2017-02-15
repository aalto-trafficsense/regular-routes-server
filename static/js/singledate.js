$(document).ready(function() {
    var date = $('#date');
    date.pickadate({
	clear: '',
	firstDay: 1,
	format: 'dd.mm.yyyy',
	hiddenName: true,
	onClose: function() {
	    location.hash = this.get('select', 'yyyy-mm-dd');
	    date.blur();
	}
    });

    function update(newdate) {
        date.pickadate("picker").set("select", Date.parse(newdate));
    }

    function hashchange() {
        var date = location.hash.split("#").splice(-1)[0];
        var today = (new Date()).toISOString().slice(0, 10);
        update(date || today);
    }

    $(window).on("hashchange", hashchange);
    hashchange();
});
