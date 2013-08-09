%% -------------------------------------------------------------------
%%
%% riak_kv_mecha_backend: riak mecha backend
%%
%% Copyright (c) 2013 John Muellerleile @jrecursive  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%

% @doc riak_kv_mecha_backend is a multipurpose riak storage backend.

-module(riak_kv_mecha_backend).
-behavior(riak_kv_backend).
-author('John Muellerleile <jmuellerleile@gmail.com>').

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2, 
         stop/1,
         get/3, 
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3,

         bucket_coverage/1,
         bkey_preflist/2,
         stream_processor/4]).

-record(state, {mecha_node, partition, callback_pid}).

-type state() :: #state{}.
-type config() :: [].

% api_version
-define(API_VERSION, 1).
-define(CAPABILITIES, []).

%% JI_PLACEHOLDER ensures messages that would otherwise consist
%%  solely of [Partition] do not get interpreted as OtpErlangString
%%  in the java receiver.
-define(JI_PLACEHOLDER, '_').

%%
%% mecha-specific backend functions (called via RPC)
%%

bucket_coverage(Bucket) ->
    VNodeSelector = allup,
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),
    PVC = 1,
    ReqId = erlang:phash2(erlang:now()),
    NodeCheckService = riak_kv,
    {Nodes, _} = riak_core_coverage_plan:create_plan(VNodeSelector,
                                                     NVal,
                                                     PVC,
                                                     ReqId,
                                                     NodeCheckService),
    Nodes.

bkey_preflist(Bucket, Key) ->
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    BKey = {Bucket, Key},
    DocIdx = riak_core_util:chash_key(BKey),
    N = proplists:get_value(n_val,BucketProps),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    riak_core_apl:get_apl_ann(DocIdx, N, Ring, UpNodes).

%%
%% standard backend functions
%%

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start the partition
-spec start(integer(), config()) -> {ok, state()}.
start(Partition, Config) ->
    io:format("start ~p~n", [Partition]),
    put(mecha_node, config_value(mecha_node, Config, 'mecha_ji@127.0.0.1')),
    put(p, Partition),
    
    io:format("~p: start(~p) -> ~p~n", [?MODULE, Partition, get(mecha_node)]),
    wait_for_mecha_node(get(mecha_node)),
    case call_mecha(kv_store, start, [Partition, self()]) of
        ok -> {ok, #state { mecha_node=get(mecha_node), 
                            partition=Partition }};
        _  -> {error, cannot_contact_mecha}
    end.

%% @doc Stop the partition
-spec stop(state()) -> ok.
stop(#state { mecha_node=_MechaNode, partition=Partition}) -> 
    io:format("stop ~p~n", [Partition]),
    cast_mecha(kv_store, stop, [Partition, ?JI_PLACEHOLDER]),
    ok.
    
%% @doc Retrieve an object from the mecha backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, State = #state { mecha_node=_MechaNode, partition=Partition }) ->
    case call_mecha(kv_store, get, [Partition, Bucket, Key]) of
        {error, not_found} ->
            {error, not_found, State};
        {ok, Value0} ->
            {ok, term_to_binary(riak_object:from_json(mochijson2:decode(Value0))), State}
    end.

%% @doc Insert an object into the partition.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, Key, _IndexSpecs, Value, State = #state { mecha_node=_MechaNode, partition=Partition }) ->
    RObj = binary_to_term(Value),
    Value1 = list_to_binary(mochijson2:encode(riak_object:to_json(RObj))),
    case call_mecha(kv_store, put, [Partition, Bucket, Key, Value1]) of
        ok ->
            {ok, State};
        error ->
            {error, mecha_put_error, State}
    end.

%% @doc Delete an object from the partition
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()}.
delete(Bucket, Key, _IndexSpecs, State = #state { mecha_node=_MechaNode, partition=Partition }) ->
    case call_mecha(kv_store, delete, [Partition, Bucket, Key]) of
        ok ->
            {ok, State};
        error ->
            {error, mecha_error, State}
    end.

-spec is_empty(state()) -> boolean().
is_empty(#state { mecha_node=_MechaNode, partition=Partition }) ->
    io:format("is_empty ~p~n", [Partition]),
    call_mecha(kv_store, is_empty, [Partition, ?JI_PLACEHOLDER]).

%% @doc Get status.
-spec status(state()) -> [{atom(), term()}].
status(_State) ->
    [{mecha, awesome}].

api_version() ->
    {?API_VERSION, ?CAPABILITIES}.

fold(FoldType, TriggerFun, State = #state{ mecha_node=_MechaNode, partition=_Partition }, Fun0, Acc) ->
    Ref = make_ref(),
    StreamToPid = spawn(?MODULE, stream_processor, [[{reply_pid, self()},
                                                     {reply_ref, Ref}], 
                                                    FoldType, 
                                                    Fun0, 
                                                    Acc]),
    TriggerFun(StreamToPid),
    receive
        {Ref, done, Result} ->
            {ok, Result};
        {Ref, error, Err} ->
            io:format("~p: fold: error: Ref = ~p, Err = ~p~n", [?MODULE,
                                                                Ref,
                                                                Err]),
            {error, Err, State};
        {Ref, Err} ->
            io:format("~p: fold: error: Ref = ~p, Err = ~p~n", [?MODULE,
                                                                Ref,
                                                                Err]),
            {error, Err, State}
    end.

%% @doc Fold over all the buckets.
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()}.
fold_buckets(FoldBucketsFun, Acc, _Opts, State = #state{ mecha_node=_MechaNode, partition=Partition }) ->
    fold(fold_buckets, 
         fun(StreamToPid) -> cast_mecha(kv_store, fold_buckets, [Partition, StreamToPid]) end,
         State,
         FoldBucketsFun,
         Acc).

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, State = #state{ mecha_node=_MechaNode, partition=Partition }) ->
    Bucket =  proplists:get_value(bucket, Opts),
    case Bucket of
        undefined ->
            fold(fold_keys, 
                 fun(StreamToPid) -> cast_mecha(kv_store, fold_keys, [Partition, StreamToPid]) end,
                 State,
                 FoldKeysFun,
                 Acc);
        _ ->
            fold(fold_keys, 
                 fun(StreamToPid) -> cast_mecha(kv_store, fold_keys, [Partition, Bucket, StreamToPid]) end,
                 State,
                 FoldKeysFun,
                 Acc)
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, State = #state{ mecha_node=_MechaNode, partition=Partition }) ->
    io:format("fold_objects ~p~n", [Partition]),
    Bucket =  proplists:get_value(bucket, Opts),
    case Bucket of
        undefined ->
            fold(fold_objects, 
                 fun(StreamToPid) -> cast_mecha(kv_store, fold_objects, [Partition, StreamToPid]) end,
                 State,
                 FoldObjectsFun,
                 Acc);
        _ ->
            fold(fold_objects, 
                 fun(StreamToPid) -> cast_mecha(kv_store, fold_objects, [Partition, Bucket, StreamToPid]) end,
                 State,
                 FoldObjectsFun,
                 Acc)
    end.

%% @doc Delete all objects from this partition
-spec drop(state()) -> {ok, state()}.
drop(State = #state{ mecha_node=_MechaNode, partition=Partition }) ->
    io:format("drop ~p~n", [Partition]),
    call_mecha(kv_store, drop, [Partition, ?JI_PLACEHOLDER]),
    {ok, State}.
    
%% Ignore callbacks for other backends so multi backend works
%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

wait_for_mecha_node(MechaNode) ->
    case net_adm:ping(MechaNode) of
        pong -> 
            io:format("mecha node responding ok!~n"),
            ok;
        pang -> 
            io:format("wait_for_mecha_node: ~p down, retrying...~n",
                [MechaNode]),
            timer:sleep(1000), 
            wait_for_mecha_node(MechaNode)
    end.
    
cast_mecha(Subsystem, Cmd, Args) ->
    Ref = make_ref(),
    {Subsystem, get(mecha_node)} ! {Cmd, Args, self(), Ref},
    Ref.

call_mecha(Subsystem, Cmd, Args) ->
    Ref = cast_mecha(Subsystem, Cmd, Args),
    receive 
        {Ref, Result} -> Result
    after 1000 ->
        io:format("call_mecha: error:~n to:~p Subsystem:~p Cmd:~p Args:~p~n",
                  [get(mecha_node), Subsystem, Cmd, Args]),
        io:format("re-establishing link and retrying request...~n"),
        wait_for_mecha_node(get(mecha_node)),
        call_mecha(Subsystem, Cmd, Args)
    end.

%%
%% stream_processor is spawned for fold, etc. when the
%%  java side streams results back for processing.  Not
%%  ideal but good enough for 0.1.
%%

stream_processor(Config, fold_buckets, Fun0, Acc) ->
    receive
        Bucket when is_binary(Bucket) ->
            erlang:garbage_collect(),
            Acc0 = Fun0(Bucket, Acc),
            stream_processor(Config, fold_buckets, Fun0, Acc0);
        done ->
            ReplyPid = proplists:get_value(reply_pid, Config),
            ReplyRef = proplists:get_value(reply_ref, Config),
            ReplyPid ! {ReplyRef, done, Acc}
        after 60000 ->
            ReplyPid = proplists:get_value(reply_pid, Config),
            ReplyRef = proplists:get_value(reply_ref, Config),
            ReplyPid ! {ReplyRef, error, timeout}
    end;

stream_processor(Config, fold_keys, Fun0, Acc) ->
    receive
        {Bucket, Key} ->
            erlang:garbage_collect(),
            Acc0 = Fun0(Bucket, Key, Acc),
            stream_processor(Config, fold_keys, Fun0, Acc0);
        done ->
            ReplyPid = proplists:get_value(reply_pid, Config),
            ReplyRef = proplists:get_value(reply_ref, Config),
            ReplyPid ! {ReplyRef, done, Acc}
        after 60000 ->
            ReplyPid = proplists:get_value(reply_pid, Config),
            ReplyRef = proplists:get_value(reply_ref, Config),
            ReplyPid ! {ReplyRef, error, timeout}
    end;

stream_processor(Config, fold_objects, Fun0, Acc) ->    
    receive
        {Bucket, Key, Value0} ->
            erlang:garbage_collect(),
            Value = term_to_binary(riak_object:from_json(mochijson2:decode(Value0))),
            Acc0 = Fun0(Bucket, Key, Value, Acc),
            stream_processor(Config, fold_objects, Fun0, Acc0);
        done ->
            io:format("fold_objects: done~n"),
            ReplyPid = proplists:get_value(reply_pid, Config),
            ReplyRef = proplists:get_value(reply_ref, Config),
            ReplyPid ! {ReplyRef, done, Acc}
        after 60000 ->
            ReplyPid = proplists:get_value(reply_pid, Config),
            ReplyRef = proplists:get_value(reply_ref, Config),
            ReplyPid ! {ReplyRef, error, timeout}
    end.

config_value(Key, Config, Default) ->
    case proplists:get_value(Key, Config) of
        undefined ->
            app_helper:get_env(riak_kv, Key, Default);
        Value ->
            Value
    end.

%%
%% Test
%%
-ifdef(TEST).

simple_test() ->
    riak_kv_backend:standard_test(?MODULE, []).

-endif. % TEST
