
var panels = [];

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
    metricRow.attr("class", "metric-" + rowNum);
    $("#host-" + host1 +" .metric-rows").append(metricRow);
    layout();
}

function dashboard_element(host, row, col) {
    row = row+1;
    var host1 = hex_md5(host);
    return $("#host-" + host1 + 
             " .metric-" + row + 
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
    $(sparkline).sparkline(data, {height: '4em', width: '150%'});
    layout();
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
    dashboard_add_host_row("127.0.0.1");
    dashboard_add_metric_row("127.0.0.1");
    dashboard_add_metric_row("127.0.0.1");
    dashboard_add_metric_row("127.0.0.1");
    dashboard_metric("127.0.0.1", 1, 3, "test.label", [0, 1, 12, 15, 20, 9, 0, 1, 12, 15, 20, 9, 5]);
    dashboard_metric("127.0.0.1", 0, 11, "test2.label", [0, 1, 12, 15, 20, 9, 0, 1, 12, 15, 20, 9, 5]);
    
    console.log(dashboard);
});

$(window).resize(layout);

function layout() {
    setTimeout("_layout();", 10);
}

function _layout() {
    var w = $(".metric").width();
    var h = w * (3/4);
    $(".surface").height(h);
    $(".dynamicsparkline canvas").css("height", h*.6).css("width", w*.8);
}
