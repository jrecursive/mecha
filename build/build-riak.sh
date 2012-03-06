cd build
tar zxvf riak-1.1.1rc1.tar.gz
cp ../src/erlang/riak_kv_mecha_backend.erl riak-1.1.1rc1/deps/riak_kv/src/.
cd riak-1.1.1rc1
make rel
cd ..
mkdir app
cd app
cp -R ../riak-1.1.1rc1/rel/riak/* .
cp ../../templates/*.template etc/.
cd ..
cd ..

