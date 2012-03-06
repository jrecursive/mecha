
all: riak mecha

riak:
	@build/build-riak.sh

mecha:
	@ant

clean:
	build/clean-riak.sh
	ant clean


