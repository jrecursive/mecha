/*
 * Entry point for interacting with Mecha
 * @param baseUrl - URL for http interface (optional, default: '/mecha')
*/
function MechaClient(baseUrl) {
    if (baseUrl === undefined) {
        baseUrl = '/';
    } else {
        if (baseUrl[0] !== '/') {
            baseUrl = '/' + baseUrl;
        }
        if ((baseUrl.slice(-1) !== '/')) {
            baseUrl += '/';
        }
    }
    this.baseUrl = baseUrl;
    console.log("baseUrl = " + baseUrl);
};

MechaClient.prototype._macro = function(macro) {
    return this.baseUrl + "mecha/" + macro;
};

MechaClient.prototype._proc = function(macro) {
    return this.baseUrl + "proc/" + macro;
};

MechaClient.prototype.select = function(params, f) {
    jQuery.ajax({
        url: this._macro('select'),
        crossDomain:true,
        type: 'GET',
        data: params,
        dataType: 'text',
        success: function(text, statusText) {
            var data = JSON.parse(text);
            console.log("elapsed: " + data.elapsed);
            f(data);
        }
    });
};

MechaClient.prototype.facet = function(params, f) {
    jQuery.ajax({
        url: this._macro('facet'),
        crossDomain:true,
        type: 'GET',
        data: params,
        dataType: 'text',
        success: function(text, statusText) {
            var data = JSON.parse(text);
            console.log("elapsed: " + data.elapsed);
            f(data);
        }
    });
};

MechaClient.prototype.commit = function(f) {
    jQuery.ajax({
        url: this._macro('commit'),
        crossDomain:true,
        type: 'GET',
        data: {},
        dataType: 'text',
        success: function(text, statusText) {
            var data = JSON.parse(text);
            console.log("elapsed: " + data.elapsed);
            f(data);
        }
    });
};

MechaClient.prototype.globalBucketCount = function(f) {
    jQuery.ajax({
        url: this._macro('global-count'),
        crossDomain:true,
        type: 'GET',
        data: { "type": "bucket" },
        dataType: 'text',
        success: function(text, statusText) {
            var data = JSON.parse(text);
            console.log("elapsed: " + data.elapsed);
            f(data);
        }
    });
};

MechaClient.prototype.globalPartitionCount = function(f) {
    jQuery.ajax({
        url: this._macro('global-count'),
        crossDomain:true,
        type: 'GET',
        data: { "type": "partition" },
        dataType: 'text',
        success: function(text, statusText) {
            var data = JSON.parse(text);
            console.log("elapsed: " + data.elapsed);
            f(data);
        }
    });
};

MechaClient.prototype.hostFacet = function(params, f) {
    jQuery.ajax({
        url: this._macro('host-facet'),
        crossDomain:true,
        type: 'GET',
        data: params,
        dataType: 'text',
        success: function(text, statusText) {
            var data = JSON.parse(text);
            console.log("elapsed: " + data.elapsed);
            f(data);
        }
    });
};
