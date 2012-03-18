var mechaClient = new MechaClient();
var config;

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
                f(obj.o);
                c.close();
            } else if (obj.o["$"] &&
                obj.o["$"] == "cancel") {
                errorHandler();
                c.close();
            }
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

var last_key = "*";
var previous_bucket_start = 0;
var bucket_start = 0;
function browse(bucket, limit) {
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
            "filter": "key:[" + last_key + " TO *]",
            "limit": limit
        }, function(result) {
            console.log(result);
            var rs = result.result; // [0].data;
            console.log(rs);
            var fields = {};
            for(idx in rs) {
                var data = rs[idx];
                for(field in data) {
                    if (field.charAt(0) == "$") continue;
                    if (field == "key" ||
                        field == "bucket") continue;
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
            for(idx in rs) {
                var data = rs[idx];
                last_key = data.key;
                console.log(data);
                var result_html = "<tr>";
                result_html += "<td>" + idx + "</td>";
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
            
            $("#browse-content").html("");
            var table_html = "<div class='table-container'>" + table.html() + "</div>";
            $("#browse-blurb").html(
                "Showing rows " + 
                    bucket_start + "-" + 
                    (bucket_start+rec_count) + " of " +
                    Math.floor((buckets[bucket].count/props.n_val)));
            console.log(buckets[bucket]);
            $("#browse-content").html(table_html);
            layout();
            previous_bucket_start = bucket_start;
            bucket_start += limit;
        });
    });
}

function structure(bucket) {
    
    
    $("#structure").text(bucket);
}

function statistics(bucket) {
    
    
    $("#statistics").text(bucket);
}

/*
 * 
 * mechanics
 *
*/

function bucket_click() {
    var bucket = $(this).text();
    load_bucket(bucket);
    console.log("bucket click: " + bucket);
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
        console.log(result);
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
    $("#older").click(function() {
        console.log("older");
        older();
    });
    $("#newer").click(function() {
        console.log("newer");
        newer();
    });
    load();
});

function older() {
    if (previous_bucket_start == 0) {
        return;
    }
}

function newer() {

}

function load_bucket(bucket) {
    last_key = "*";
    bucket_start = 0;
    browse(bucket, 30);
    structure(bucket);
    statistics(bucket);
}

function layout() {
    // table-container
    var h = $(window).height();
    $(".table-container").height(h-200);
}

function load() {
    bucket_list();
}