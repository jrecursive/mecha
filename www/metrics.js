
var log_triggered = false;
prevdata = {};

function getMetrics() {
    var writtenHeader = false;
    var html = "<table class='table table-bordered table-condensed'>";
    $.getJSON('/proc/metrics?entries=30&all=true', function(data) {
        metrics = data.result;
        names = new Array();
        dnames = new Array();
        var td_max = 7;
        var td_c = 0;
        for(var host in data.result) {
            if (!log_triggered) {
                getLog(host);
            }
            html += "<tr>";
            for(var name in data.result[host]) {
                if(name.indexOf("riak.memory") >= 0 &&
                   name.indexOf("riak.memory.total") != 0) continue;
                if(name.indexOf("fsm.objsize") > 0) continue;
                if(name.indexOf("fsm.siblings") > 0) continue;
                if(name.indexOf("riak.pbc") >= 0) continue;
                if(name.indexOf("vnode.index") > 0) continue;
                if(name.indexOf("fsm.time") > 0) continue;
                if(name.indexOf("read_rep") > 0) continue;
                names.push(name);
            }
            names.sort();
            for(var idx in names) {
                var name = names[idx];
                parts = name.split(".");
                var dname = 
                    parts[parts.length-2] + "." + 
                    parts[parts.length-1]
                dnames.push(dname);
            }
            html += "<td style='vertical-align:middle; horizontal-align:center; width:" + Math.ceil(100/(td_max+1)) + "%'><center>" + host + "</center></td>";
            
            td_c = 0;
            for (var i=0; i<names.length; i++) {
                var name = names[i];
                var dname = dnames[i];
                data.result[host][name].values.reverse();
                var metric = data.result[host][name];
                cur_val = metric.values[metric.values.length-1];
                //if (cur_val == 0) continue;
                if (prevdata[host] !== null &&
                    prevdata[host] !== undefined) {
                    try {
                        pre_val = prevdata[host][name].values[prevdata[host][name].values.length-1];
                        if (pre_val == 0 &&
                            cur_val > 0) {
                            bgcol = "rgba(0, 255, 0, .1)";
                        } else {
                            
                            if (cur_val == pre_val) {
                                bgcol = "rgb(255,255,255)";
                            } else if (cur_val > pre_val) {
                                var cv = 1 - (pre_val / cur_val);
                                bgcol = "rgba(255,0,0," + cv + ")";
                            } else if (cur_val < pre_val) {
                                var cv = (cur_val / pre_val) * 0.15;
                                bgcol = "rgba(0,186,255," + cv +")";
                            }
                            console.log(bgcol);
                        }
                    } catch (ex) {
                        bgcol = "white";
                    }
                } else {
                    bgcol = "white";
                }
                
                if (!data.result[host][name]) {
                    html += "<td>n/a</td>";
                    continue;
                }
                
                html += "<td style='vertical-align:middle; background:" + bgcol + "; " +
                        "width:" + Math.ceil(100/(td_max+1)) + "%'>" + 
                        "<center><a class='lbl' style='color:black;' href='#' rel='tooltip' title='" + name + ": " + metric.values[metric.values.length-1] + "'>" + dname + "</a><br>";
                html += "<span style='display:inline;' class='dynamicsparkline'>";
                for(var j=0; j<metric.values.length; j++) {
                    var value = metric.values[j];
                    html += value;
                    if (j < metric.values.length-1) html += ",";
                }
                html += "</span></center> ";
                //html += "<span class='lbl'>" + metric.values[metric.values.length-1] + "</span>";
                
                /*
                html += "<span class='value'>" + 
                    metric.values[metric.values.length-1] + "<br/>" +
                    Math.ceil(metric["95th"]) + "<br/>" +
                    Math.ceil(metric["mean"]) + "<br/>" +
                    "</span>";
                */
                
                html += "</td>";
                td_c++;
                if (td_c == td_max) {
                    td_c = 0;
                    html += "</tr><tr><td>&nbsp;</td>";
                }
            }
            if (td_c < td_max+1) {
                for(i=td_c; i<td_max; i++) {
                    html += "<td>&nbsp;</td>";
                }
            }
            html += "</tr>\n";
            prevdata[host] = data.result[host];
        }
        html += "</table>\n";
        $("#metrics").html(html);
        $(".dynamicsparkline").sparkline();
        $("div.tooltip").remove();
        $("a.lbl").tooltip({ "trigger": "hover" });
        setTimeout("getMetrics();", 500);
    });
}

xhr = 0;
log_ok = false;
function getLog(host) {
    if (!log_ok &&
        xhr != 0) {
        console.log("xhr abort");
        xhr.abort();
    }
    var url = "/mecha/system-select?host=" + 
        host + 
        "&sort=last_modified desc&limit=10&filter=bucket:log AND NOT name:riak.log";
    log_ok = false;
    xhr = $.getJSON(url, function(data) {
        log_triggered = true;
        var html = "<table class='table table-bordered table-bordered table-condensed'>";
        var ex_ct = 0;
        for(var i=0; i<data.result.length; i++) {
            var result = data.result[i];
            if (result['message'] == "") continue;
            if (result['is_error_b']) {
                modifier = "style='background:yellow'";
                label_mod = "<span class='label label-warning'>Exception</span>";
            } else {
                modifier = "";
                label_mod = "";
            }
            
            if (result['short_message_t']) {
                short_message = 
                    "<p>" + result['short_message_t'] + "</p>";
            } else {
                short_message = "";
            }
            
            if (result['is_error_b']) {
                trace_block = "<div id='ex_" + ex_ct + "'>&nbsp;</div>";
            } else {
                trace_block = "";
            }
            
            if (result['name'] == 'riak.log') {
                message_html = "<span>" + result['message'] + "</span>";
            } else {
                message_html = "<h4>" + result['message'] + "</h4>";
            }
            
            var result_t = moment(result['last_modified'], "YYYY-MM-DDTHH:mm:ss").subtract('hours', 1).fromNow()
            html += 
                "<tr>" +
                    "<td " + modifier + " >" + result['name'] + "<br/>" + label_mod + "<br/></td><td>" +
                        message_html +
                        short_message +
                        trace_block +
                    "</td><td style='width:100px; vertical-align:top;'>" + result_t + "</td>" +
                "</tr>";
            if (trace_block != "") {
                getTrace(host, result['trace_id_s'], ex_ct);
                ex_ct++;
            }
        }
        html += "</table>";
        $("#log").html(html);
        log_ok = true;
    })
    setTimeout("getLog('" + host + "');", 15000);
}

function getTrace(host, trace_id, ex_ct) {
    var url = '/mecha/system-select?host=' + host + '&sort=last_modified%20asc&limit=100&filter=trace_id_s:' + trace_id;
    xhr = $.getJSON(url, function(data) {
        var obj = data.result;
        var html = "<table class='table table-bordered table-condensed'>";
        for(i=0; i<obj.length; i++) {
            var o = obj[i];
            if (!o["class_s"]) {
                continue;
            }
            if (o["class_s"].indexOf("mecha") != 0) continue;
            html += '<tr><td><b>' + o['class_s'] + "</b></td><td>" + o['filename_s'] + "</td><td>" + o['line_number_i'] + "</td></tr>";
        }
        html += "</table>";
        console.log(html);
        $("#ex_" + ex_ct).html(html);
    });
}

$(document).ready(function() {
    getMetrics();
});
