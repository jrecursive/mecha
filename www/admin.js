
var panels = [];
var dashboard_worker, console_worker, log_worker;

function panel(p) {
    for(i=0; i<panels.length; i++) {
        var panelName = panels[i];
        var panel = $("#" + panelName);
        if (panelName == p) {
            panel.show();
        } else {
            panel.hide();
        }
    }
    layout();
}

/*
 * dashboard
*/

var dashboard = {    
    /*
     * { "host": "127.0.0.1": {
     *      "rows": 1 
     *   },
     *   ...
     * }
    */
    "hosts": {}
};

function dashboard_add_host_row(host) {
    var host1 = hex_md5(host);
    if ($("#host-" + host1).length > 0) return;
    var hostRow = $("#host-row-template").clone();
    hostRow.attr("id", "host-" + host1);
    $(hostRow).find(".host-label").html(host);
    $("#Dashboard").append(hostRow);
    dashboard.hosts[host] = { "rows":0, "hash": host1 };
    layout();
}

function dashboard_add_metric_row(host) {
    var host1 = hex_md5(host);
    if ($("#host-" + host).length == 0) {
        dashboard_add_host_row(host);
    }
    var rowNum = dashboard.hosts[host].rows + 1;
    dashboard.hosts[host].rows = rowNum;
    var metricRow = $("#metric-row-template").clone();
    metricRow.attr("id", host1 + "-metric-" + rowNum);
    metricRow.attr("class", "metric-" + rowNum);
    $("#host-" + host1 +" .metric-rows").append(metricRow);
    layout();
}

function dashboard_element(host, row, col) {
    row = row+1;
    var host1 = hex_md5(host);
    return $("#host-" + host1 + 
             " .metric-" + row + 
             " .metric-row" +
             " .col-" + col + 
             " .surface");
}

function dashboard_metric(host, row, col, label, data) {
    var element = dashboard_element(host, row, col);
    var metric = $("#metric-template").clone();
    metric.attr("id", "m-" + label);
    var sparkline = $("#sparkline-template span.dynamicsparkline").clone();

    $(metric).find(".metric-label").html(label);
    $(metric).find(".metric-sparkline").append(sparkline);
    element.html("");
    element.append(metric);
    layout();
    $(sparkline).sparkline(data);
    layout();
}

function metric_color(host, row, col, toRGBA) {
    var element = dashboard_element(host, row, col);
    
    var bgColorStr = "rgba(" + 
        toRGBA.r + "," +
        toRGBA.g + "," +
        toRGBA.b + "," +
        toRGBA.a + ")";
    element.css("background-color", bgColorStr);
}

function metric_scale(host, row, col, fromScale, toScale) {
    var element = dashboard_element(host, row, col);
    var scaleTween = 
        new Tween(new Object(),
                  'xyz',
                  Tween.linear,
                  fromScale, toScale, .1);
    scaleTween.onMotionChanged = function(event) { 
        var val = event.target._pos;
        apply_transform(element, "scale(" + val + "," + val + ")");
    };
    scaleTween.start();
}

function metric_alpha(host, row, col, alpha) {
    dashboard_element(host, row, col).css("opacity", alpha);
}

function apply_transform(el, transform) {
    el.css("-moz-transform", transform)
      .css("-webkit-transform", transform)
      .css("-ms-transform", transform)
      .css("transform", transform);
}

/*
 * -- dashboard setup & web worker functions --
*/

function rlog(msg) {
    console.log(msg);
}

function dashboard_setup() {
    dashboard_worker = new Worker("worker.dashboard.js");
    dashboard_worker.addEventListener('message', function(e) {
        var req = e.data;
        var scope = req.scope;
        var fun = req.fun;
        var args = req.args;
        eval("var func = " + fun + ";");
        func.apply(scope, args);
        //console.log('Worker said: ', e.data);
    }, false);
    dashboard_refresh();
}

function dashboard_refresh() {
    $.getJSON('/proc/metrics?entries=25&all=true', function(data) {
        dashboard_worker.postMessage(data);
    });
}

function dashboard_rows(host) {
    if (!dashboard.hosts[host]) {
        dashboard_add_host_row(host);
    }
    return dashboard.hosts[host].rows;
}

function d_ensure_rows(host, rows) {
    while(dashboard_rows(host) < rows) {
        dashboard_add_metric_row(host);
    }
}


/*
 * console
*/

/*
 * log
*/

$(document).ready(function() {
    $("a.nav-tab").each(function(_idx) {
        var tabName = $(this).text();
        panels.push(tabName);
        $(this).click(function() {
            $(".nav-tab").parent().removeClass("active");
            $(this).parent().addClass("active");
            panel(tabName)
        });
    });
    panel('Dashboard');
    /*
    dashboard_add_host_row("127.0.0.1");
    dashboard_add_metric_row("127.0.0.1");
    dashboard_add_metric_row("127.0.0.1");
    dashboard_add_metric_row("127.0.0.1");
    dashboard_metric("127.0.0.1", 1, 3, "test.label", [0, 1, 12, 15, 20, 9, 0, 1, 12, 15, 20, 9, 5]);
    metric_color("127.0.0.1", 1, 3, {"r":0, "g":50, "b":25, "a":0.6});
    metric_alpha("127.0.0.1", 1, 3, 0.8);
    metric_scale("127.0.0.1", 1, 3, 1, 1.2);
    dashboard_metric("127.0.0.1", 0, 11, "test2.label", [0, 1, 12, 15, 20, 9, 0, 1, 12, 15, 20, 9, 5]);
    
    setTimeout('metric_scale("127.0.0.1", 1, 3, 1.2, 1);', 3000);
    setTimeout('metric_color("127.0.0.1", 1, 3, {"r":255, "g":0, "b":0, "a":1.0});', 3001);
    
    console.log(dashboard);
    */
    
    dashboard_setup();
});

$(window).resize(layout);

function layout() {
    setTimeout("_layout();", 10);
}

function _layout() {
    var w = $(".metric").width();
    var h = w * (3/4) * .9;
    $(".surface").height(h);
    //$(".dynamicsparkline canvas").css("height", h*.6).css("width", w*.8);
}
