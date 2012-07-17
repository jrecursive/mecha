cd build

if [ ! -d riak-1.2.0rc1 ];
then
	tar zxvf riak-1.2.0rc1.tar.gz
	cp ../src/erlang/riak_kv_mecha_backend.erl riak-1.2.0rc1/deps/riak_kv/src/.
	cd riak-1.2.0rc1
	make rel
if [ ! -d rel/riak ];
then
	echo UNABLE TO BUILD RIAK!
	cd ..
	./clean-riak.sh
else
	cp ../templates/*.template rel/riak/etc/.
	cd ..
	ln -s riak-1.2.0rc1/rel/riak riak
	cd ..
fi
fi

