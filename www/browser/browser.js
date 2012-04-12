var mechaClient = new MechaClient();
var config;
var g_limit = 30;

function do_cmd(cmd, f) {
    var c = new WebSocket("ws://" + config['server-addr'] + ":" + 
            config['websocket-port'] + "/mecha");
    c.onopen = function() {
        c.send("auth " + config.password);
        c.send(cmd);
    }
    c.onmessage = function(msg0) {
        msg=msg0.data;
        if (msg.charAt(0) == '{') {
            var obj = $.parseJSON(msg);
            if (!obj) {
                c.close();
                return;
            } else if (!obj.c) {
                if (obj.error) {
                    log("error: " + obj.error);
                    errorHandler(obj);
                    c.close();
                    return;
                }
                c.close();
                return;
            }
            if (obj.o["$"] &&
                obj.o["$"] == "done") {
                c.close();
                return;
            } else if (obj.o["$"] &&
                obj.o["$"] == "cancel") {
                errorHandler();
                c.close();
                return;
            }
            f(obj.o);
        } else {
            console.log(msg);
        }
    };
    c.onclose = function(){  
         log('do_cmd: disconnected ('+cmd+')');
    }
}

/*
 * query notes
 *
 * mdFacet:
 *  /mecha/host-facet?field=partition&filter=bucket:st_terms
 *    number of records per partition for a given bucket (including replicas)
 *
 *  /mecha/host-facet?field=bucket
 *    number of records per host for a given bucket (including replicas)
 *
 *  /mecha/count?bucket=st_terms
 *    number of records for a given bucket (NOT including replicas)
 *
 *  /mecha/bucket-props?bucket=st_terms
 *    riak bucket properties: 
 *
 *  /mecha/select-set?bucket=geo&materialize=true&sort-field=key&sort-dir=asc&start=0&limit=10
 *    reliable but with "ragged ordering" iteration through a bucket
 *   
*/

var last_key = ["*"];
var bucket_start = [0];
var c_bucket = "";
function browse(bucket, limit) {
    c_bucket = bucket;
    mechaClient.bucketProperties({
        "bucket": bucket
    }, function (props) {
        props = props.result[0].props;
        limit1 = Math.ceil(limit / props.n_val);
        var table = $("#results-table .results-table").parent().clone();
        mechaClient.select({
            "bucket": bucket,
            "materialize": false,
            "sort-field": "key",
            "sort-dir": "asc",
            "filter": "key:[" + last_key[last_key.length-1] + " TO *]",
            "limit": limit
        }, function(result) {
            var rs = result.result; // [0].data;
            var fields = {};
            for(idx in rs) {
                var data = rs[idx];
                for(field in data) {
                    if (field.charAt(0) == "$") continue;
                    if (field == "key" ||
                        field == "bucket" ||
                        field == "partition" ||
                        field == "vclock" || 
                        field == "vtag" || 
                        field == "last_modified" ||
                        field == "id") continue;
                    if (data[field] == "") continue;
                    fields[field] = 1;
                }
            }
            var field_list = [];
            for(field in fields) {
                field_list.push(field);
            }
            field_list.sort();
            for(idx in field_list) {
                var field = field_list[idx];
            }
            field_list.reverse();
            field_list.push("key");
            field_list.reverse();
            var hdr_row = table.find(".header-result-row");
            var hdr_html = "<td>#</td>";
            for(idx in field_list) {
                var field = field_list[idx];
                hdr_html += "<td>" + field + "</td>";
            }
            hdr_row.html(hdr_html);
            var result_body = table.find(".result-rows-body");
            var rec_count = 0;
            var b_start = bucket_start[bucket_start.length-1];
            for(idx in rs) {
                var data = rs[idx];
                last_key1 = data.key;
                var result_html = "<tr>";
                result_html += "<td>" + (b_start + rec_count + 1) + "</td>";
                for(idx in field_list) {
                    var field = field_list[idx];
                    result_html += 
                        "<td>" +
                        data[field] + 
                        "</td>";
                }
                result_html += "</tr>";
                result_body.append(result_html);
                rec_count++;
            }
            last_key.push(last_key1);
            
            $("#browse-content").html("");
            var table_html = "<div class='table-container'>" + table.html() + "</div>";
            $("#browse-blurb").html(
                "Showing rows " + 
                    (b_start+1) + "-" + 
                    (b_start+rec_count) + " of " +
                    Math.floor((buckets[bucket].count/props.n_val)));
            $("#browse-content").html(table_html);
            layout();
            bucket_start.push(b_start+limit);
        });
    });
}

function structure(bucket) {
    var table = $("#results-table .results-table").parent().clone();
    table.attr("id", "structure-table");
    mechaClient.deriveSchema({
        "max-samples": 10,
        "bucket": bucket
    }, function(data) {
        var schema = data.result[0];
        var hdr_row = table.find(".header-result-row");
        hdr_row.html("<tr><td>field</td><td>%</td></tr>");
        var result_body = table.find(".result-rows-body");
        var max_occ = 0;
        var bucket_fields = [];
        for(field in schema) {
            var field_occ = schema[field];
            if (field_occ > max_occ) max_occ = field_occ;
            bucket_fields.push(field);
        }
        for(field in schema) {
            pct = schema[field] / max_occ * 100.00;
            var row_html = 
                "<tr><td>" + 
                field + 
                "</td><td>" +
                pct + 
                "%</td></tr>";
            result_body.append(row_html);
        }
        var table_html = "<div class='table-container'>" + table.html() + "</div>";
        $("#structure-content").html(table_html);
        //statistics(bucket, bucket_fields);
    });
}

function statistics(bucket, fields) {
    $("#statistics-content").html("");
    var table = $("#results-table .results-table").parent().clone();
    table.attr("id", "statistics-table");
    var hdr_row = table.find(".header-result-row");
    hdr_row.html("<tr><td>field</td><td>cardinality</td></tr>");
    var result_body = table.find(".result-rows-body");
    var max_occ = 0;
    for(idx in fields) {
        var field = fields[idx];
        var row_html = 
            "<tr><td>" + 
            field + 
            "</td><td><div id='" +
            hex_md5("cardinality-" + field) +  /* encoded cardinality span id */
            "'></div></td></tr>";
        result_body.append(row_html);
    }
    var table_html = "<div class='table-container'>" + table.html() + "</div>";
    $("#statistics-content").append(table_html);
    
    for(idx in fields) {
        var field = fields[idx];
        do_stat_fc(bucket,field);
    }
}

function do_stat_fc(bucket,field) {
        mechaClient.cardinality({
            "bucket": bucket,
            "field": field
        }, function(data) {
            var data = data.result[0];
            console.log(data);
            var html = "<table class='table table-bordered'>";
            for (datafield in data['by-partition']) {
                var datafield_l = datafield.split("-")[1];
                html += 
                    "<tr><td>" + datafield_l + "</td>" +
                    "<td>" + data['by-partition'][datafield] + "</td></tr>";
            }
            html += "<tr><td colspan=2>&nbsp;</td></tr>";
            for (datafield in data) {
                if (datafield == "by-partition") continue;
                html += 
                    "<tr><td>" + datafield + "</td>" +
                    "<td width='50%'>" + data[datafield] + "</td></tr>";
            }
            html += "</table>";
            console.log("#" + hex_md5("cardinality-" + field));
            $("#" + hex_md5("cardinality-" + field)).html(html);
            console.log(html);
        });
}

/*
 * 
 * mechanics
 *
*/

function bucket_click() {
    var bucket = $(this).text();
    load_bucket(bucket);
    //console.log("bucket click: " + bucket);
}

function clear_buckets() {
    $(".bucket-list").html(
        '<li class="nav-header buckets">Buckets</li>');
}

function add_bucket(bucket) {
    if ($("#" + bucket_id(bucket)).length > 0) return;
    
    var after_el = $(".buckets");
    after_el.after(
        "<li class='bucket'>" + 
        "<a style='display:inline-block;' href='#' id='" + bucket_id(bucket) + "'>" +
        bucket + 
        "</a>" + 
        /*
        "<a style='float:right;' href='#' id='delete-" + bucket_id(bucket) + "' " +
            "class='" + bucket_id(bucket) + "'>×</a>" +
        */
        "</li>\n");
    $("#" + bucket_id(bucket)).click(bucket_click);
    //$("#delete-" + bucket_id(bucket)).click(delete_click);
}

var buckets = {};
function bucket_list() {
    buckets = {};
    clear_buckets();
    mechaClient.globalBucketCount(function(result) {
        //console.log(result);
        result = result.result;
        bucket1 = "";
        for(idx in result) {
            var obj = result[idx];
            for (bucket in obj) {
                if (bucket1 == "") {
                    bucket1 = bucket;
                }
                buckets[bucket] = { "count": obj[bucket] };
                add_bucket(bucket);
            }
        }
        load_bucket(bucket1);
    });
}

function bucket_id(term) {
    return hex_md5(term);
}

$(document).ready(function() {
    $.getJSON('/proc/config', function(data) {
        config = data.result;
    });
    $(window).resize(function() {
        layout();
    });
    $("#prev-btn").click(function() {
        prevpage();
    });
    $("#next-btn").click(function() {
        nextpage();
    });
    load();
});

function prevpage() {
    bucket_start.pop();
    last_key.pop();
    if (bucket_start.length > 1) {
        bucket_start.pop();
        last_key.pop();
    }
    browse(c_bucket, g_limit);
}

function nextpage() {
    if (bucket_start[bucket_start.length-1]+g_limit > buckets[c_bucket].count) {
        return;
    }
    browse(c_bucket, g_limit);
}

function load_bucket(bucket) {
    last_key = ["*"];
    bucket_start = [0];
    browse(bucket, g_limit);
    //structure(bucket);
    //statistics(bucket); /* depends on structure, and is called upon successful structure call */
}

function layout() {
    // table-container
    var h = $(window).height();
    var container_h = h-200;
    $(".table-container").height(container_h);
}

function load() {
    bucket_list();
}