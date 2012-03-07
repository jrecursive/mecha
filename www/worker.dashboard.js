/*
 * Dashboard background updater.
*/

var prevdata = {};
var prevscale = {};

self.addEventListener('message', function(e) {
    var result = e.data.result;
    
    var col_max = 12;
    
    for(host in result) {
        if (!prevscale[host]) {
            prevscale[host] = {};
        }
        var row_ct = 0;
        var col_ct = 0;
        var names = [];
        for(name in result[host]) {
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
        var rows = Math.ceil(names.length / 12);
        //postMessage({"scope": {}, "fun":"rlog", "args": ["rows: " + rows]});
        postMessage({"scope": {}, "fun":"d_ensure_rows", "args": [host, rows]});
        
        for(idx in names) {
            var name = names[idx];
            var metric = result[host][name];
            var cur_val = metric.values[metric.values.length-1];
            
            bgcol = { "r": 255, "g": 255, "b": 255, "a": 1.0 };
            
            var cv = 1;
            
            if (prevdata[host] !== null &&
                prevdata[host] !== undefined) {
                try {
                    pre_val = prevdata[host][name].values[prevdata[host][name].values.length-1];
                    if (pre_val == 0 &&
                        cur_val > 0) {
                        //bgcol = "rgba(0, 255, 0, .1)";
                        bgcol = {"r":0, "g":255, "b":0, "a":0.1};
                    } else {
                        if (cur_val == pre_val) {
                            bgcol = {"r":255, "g":255, "b":255, "a":1.0};
                        } else if (cur_val > pre_val) {
                            cv = 1 - (pre_val / cur_val);
                            scv = 1+(.15 * cv);
                            bgcol = {"r":255, "g":0, "b":0, "a":cv};
                        } else if (cur_val < pre_val) {
                            cv = (cur_val / pre_val) * 0.15;
                            scv = .9+cv;
                            bgcol = {"r":0, "g":186, "b":255, "a":cv};
                        }
                    }
                } catch (ex) {
                    bgcol = {"r":255, "g":255, "b":255, "a":1.0};
                }
            } else {
                bgcol = {"r":255, "g":255, "b":255, "a":1.0};
            }
            
            if (!result[host][name]) continue;
            
            // dashboard_metric("127.0.0.1", 0, 11, "test2.label", [0, 1, 12, 15, 20, 9, 0, 1, 12, 15, 20, 9, 5]);
            var label = name;
            while(label.indexOf(".")>=0) {
                label = label.replace("\.", " ");
            }
            label = label.replace("mecha ", "");
            label = label.replace("riak ", "");
            label = label.replace("global ", "");
            label = label.replace("server ", "");
            label = label.replace("node ", "");
            label = label.replace("processes ", "procs");
            label = label.replace("documents", "docs");
            label = label.replace("http macro ", "");
            label = label.replace("vm bifs ", "");
            label = label.replace("solr-module system select query ms", "select ms");
            label = label.replace("mvm functions ", "");
            label = label.replace("mvm memory-channels", "memory channels");
            label = label.replace("solr-module index ", ""); // select-iterator query ms
            label = label.replace("vindex", "");
            label = label.replace("select-iterator", "");

            postMessage({"scope": {}, 
                         "fun":"dashboard_metric", 
                         "args": [
                            host,
                            row_ct,
                            col_ct,
                            label,
                            metric.values
                         ]});
            postMessage({"scope": {}, 
                         "fun":"metric_color", 
                         "args": [
                            host,
                            row_ct,
                            col_ct,
                            bgcol
                         ]});
            
            if (!prevscale[host][name]) {
                prevscale[host][name] = 1.0;
            }
            var pscv = prevscale[host][name];
            if (cv == 1) scv = 1;
            prevscale[host][name] = scv;
            
            if (pscv != scv) { /* && 
                (scv > 1 ||
                 pscv > 1)) { */
                postMessage({"scope": {}, 
                             "fun":"metric_scale", 
                             "args": [
                                host,
                                row_ct,
                                col_ct,
                                pscv,
                                scv
                             ]});
            }
            col_ct++;
            if (col_ct == col_max) {
                col_ct = 0;
                row_ct++;
            }
        }
        prevdata[host] = result[host];
    }
    setTimeout('refresh();', 500);
}, false);

function refresh() {
    postMessage({"scope": {}, 
                 "fun":"dashboard_refresh", 
                 "args": []});
}
// 

/*
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
                        "<center><a class='lbl' style='color:black;' href='#' rel='tooltip' title='" + name + ": " + metric.values[metric.values.length-1] + "'>" + name + "</a><br>";
                html += "<span style='display:inline;' class='dynamicsparkline'>";
                for(var j=0; j<metric.values.length; j++) {
                    var value = metric.values[j];
                    html += value;
                    if (j < metric.values.length-1) html += ",";
                }
                html += "</span></center> ";
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
*/