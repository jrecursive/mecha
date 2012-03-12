
all: riak mecha

riak:
	@build/build-riak.sh

mecha:
	@ant

clean:
	build/clean-riak.sh
	ant clean

reset:
	rm -rf ./data
	rm -rf ./build/riak-1.1.1rc1/rel/riak/data
