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



/*
 * 
 * mechanics
 *
*/

function bucket_click() {
    var bucket = $(this).text();
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
        "<li>" + 
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
        for(idx in result) {
            var obj = result[idx];
            for (bucket in obj) {
                buckets[bucket] = { "count": result[bucket] };
                add_bucket(bucket);
            }
        }
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
    wire();
    layout();
    load();
    $("#q").focus();
});

function wire() {
}

function layout() {
}

function load() {
    bucket_list();
}