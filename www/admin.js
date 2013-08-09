/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * (C) 2013 John Muellerleile @jrecursive
 *
 */

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
    $("#Metrics").append(hostRow);
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
    if ($(element).find("#metric-template").length == 0) {
        var metric = $("#metric-template").clone();
        metric.attr("id", "m-" + label);
        var sparkline = $("#sparkline-template span.dynamicsparkline").clone();
        $(metric).find(".metric-sparkline").append(sparkline);
    } else {
        var metric = $(element).find("#metric-template");
        var sparkline = $(metric).find("span.dynamicsparkline");
    }

    if ($(metric).find(".metric-label").text() != label) {
        $(metric).find(".metric-label").html(label);
        element.html("");
        element.append(metric);
    }
    $(sparkline).sparkline(data);
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
    zv = Math.floor(100 * toScale);
    $(element).css("z-index", zv);
    var scaleTween = 
        new Tween(new Object(),
                  'xyz',
                  Tween.BounceEaseOut,
                  fromScale, toScale, 1);
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
    $.getJSON('/proc/metrics?entries=30&all=true', function(data) {
        dashboard_worker.postMessage(data);
    })
    .error(function() {
        console.log("xhr failure, retrying dashboard refresh..");
        setTimeout("dashboard_refresh();", 1000);
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

function layout() {
    _console_layout();
    //_layout();
}

function _console_layout() {
    var h = $(window).height()-60;
    $(".console-iframe").height(h);
}

function _layout() {
    var w = $(".metric").width();
    var h = w * (3/4) * .9;
    $(".surface").height(h);
    //$(".metric-rows .metric").css("margin-left", "10px");
}

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
    //dashboard_setup();
    $(window).resize(layout);
});

/* 
 * overview / home page
*/

/*
 * /proc/config
 * /proc/cluster/do?u=
 * /proc/node/last-commit
 * /mecha/global-select?type=bucket
 * /mecha/global-select?type=partition
 * 
*/

