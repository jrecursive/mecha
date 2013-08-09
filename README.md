

mecha
=====

### THIS IS EXPERIMENTAL, PROTOTYPE CODE AND NO LONGER MAINTAINED

## some interesting waypoints in the code

#### MVM - If you are curious about the query subsystem (arguably the interesting part) here are the places to look: 

* https://github.com/jrecursive/mecha/blob/master/mvm/bootstrap.mvm is the "mecha virtual machine" registering of "query primitives" and build-up of those primitives into higher-level queries (eg select, join, etc)

* https://github.com/jrecursive/mecha/tree/master/src/java/mecha/vm/bifs actual implementations of the query primitives (gnarly, shameful code, but like i said, this is/was a prototype) 

* https://github.com/jrecursive/mecha/blob/master/src/java/mecha/vm/MVM.java the actual "mecha virtual machine"

#### JINTERFACE - if you're a masochist and enjoy interfacing java <-> erlang: 

* https://github.com/jrecursive/mecha/blob/master/src/java/mecha/jinterface/RiakConnector.java

* https://github.com/jrecursive/mecha/blob/master/src/erlang/riak_kv_mecha_backend.erl

* mecha-specific RPC back to erlang: https://github.com/jrecursive/mecha/blob/master/src/java/mecha/jinterface/RiakRPC.java

#### STORAGE & INDEX - bucket interface and storage

* https://github.com/jrecursive/mecha/blob/master/src/java/mecha/db/Bucket.java this is probably the most interesting file, handling field name conversion, riak object decomposition, etc

* https://github.com/jrecursive/mecha/tree/master/src/java/mecha/db mecha.db.*, in particular, https://github.com/jrecursive/mecha/blob/master/src/java/mecha/db/MDB.java is where I implemented the riak bucket interface (of the time)

* https://github.com/jrecursive/mecha/blob/master/src/erlang/riak_kv_mecha_backend.erl for the riak erlang-side bucket driver

* https://github.com/jrecursive/mecha/blob/master/src/java/mecha/db/SolrEventHandlers.java solr event handlers

#### The rest of the code is basically uninteresting and/or hastily written, YMMV :)

#### BUILD INSTRUCTIONS (these will probably still work)

```

* Make sure you have:
	> erlang R14Bxx
	> jdk 1.6 or newer
	> ant

* Type:

	make

* Config (set up for 1 node, localhost only):
	
	cp config.json.template config.json

** start:

bin/mecha

** watch on dashboard:

http://localhost:7283/admin/metrics/

** start ./mecha-cli and:

* commit

(#commit)

* count

(#count bucket:geo)

* select

(#select bucket:geo filter:"feature_code_s:HLL" count:10)

* derive-schema

(#derive-schema bucket:feature_codes max-samples:10)

* facet

(#facet bucket:geo field:country_s)

* spatial-select

(#spatial-select bucket:geo field:loc_ll lat:36.2 lon:10.15 radius:5)

* join

(#join left:(type:"spatial-select" params:(bucket:geo field:loc_ll lat:36.2 lon:10.15 radius:150) join-field:feature_code_s) right:(type:select params:(bucket:feature_codes) join-field:admin2_s) project:(geo.key geo.name_s feature_codes.short_name_s))

** URLS:

* join

http://localhost:7283/join?left.type=spatial-select&left.params.bucket=geo&left.params.field=loc_ll&left.params.lat=36.2&left.params.lon=10.14&left.params.radius=10&left.join-field=feature_code_s&right.type=select&right.params.bucket=feature_codes&right.join-field=admin2_s&project=geo.key&project=geo.name_s&project=feature_codes.short_name_s

* facet

http://localhost:7283/mecha/facet?bucket=geo&field=country_s

* spatial-select

http://localhost:7283/mecha/spatial-select?bucket=geo&field=loc_ll&lat=36.2&lon=10.15&radius=5

* select

http://localhost:7283/mecha/select?bucket=geo&filter=feature_code_s:HLL&count=10

* derive-schema

http://localhost:7283/mecha/derive-schema?bucket=geo&max-samples=10

* select

http://localhost:7283/mecha/select?bucket=feature_codes&filter=admin2_s:TUND

* count

http://localhost:7283/mecha/count?bucket=geo

* commit

http://localhost:7283/mecha/commit

* generate an error (watch on dashboard)

http://localhost:7283/mecha/ponies

```

