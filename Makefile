
all: riak mecha

riak:
	build/build-riak.sh

mecha:
	ant

clean:
	build/riak-clean.sh
	ant clean


