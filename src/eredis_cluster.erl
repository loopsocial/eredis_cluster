-module(eredis_cluster).
-behaviour(application).

% Application.
-export([start/2]).
-export([stop/1]).

% API.
-export([start/0, stop/0, connect/1]). % Application Management.

% Generic redis call
-export([
    q/1,
    q/2,
    qp/1,
    qp/2,
    qw/2,
    qw/3,
    qk/2,
    qk/3,
    qa/1,
    qa/2,
    qmn/1,
    qmn/2,
    transaction/1,
    transaction_with_query_timeout/2,
    transaction/2
]).

% Specific redis command implementation
-export([flushdb/0, flushdb/1]).

 % Helper functions
-export([update_key/2]).
-export([update_hash_field/3]).
-export([optimistic_locking_transaction/3, optimistic_locking_transaction/4]).
-export([eval/4, eval/5]).
-export([log_error/1]).

-include("eredis_cluster.hrl").

-spec start(StartType::application:start_type(), StartArgs::term()) ->
    {ok, pid()}.
start(_Type, _Args) ->
    eredis_cluster_sup:start_link().

-spec stop(State::term()) -> ok.
stop(_State) ->
    ok.

-spec start() -> ok | {error, Reason::term()}.
start() ->
    application:start(?MODULE).

-spec stop() -> ok | {error, Reason::term()}.
stop() ->
    application:stop(?MODULE).

%% =============================================================================
%% @doc Connect to a set of init node, useful if the cluster configuration is
%% not known at startup
%% @end
%% =============================================================================
-spec connect(InitServers::term()) -> Result::term().
connect(InitServers) ->
    eredis_cluster_monitor:connect(InitServers).

%% =============================================================================
%% @doc Wrapper function to execute a pipeline command as a transaction Command
%% (it will add MULTI and EXEC command)
%% @end
%% =============================================================================
-spec transaction(redis_pipeline_command()) -> redis_transaction_result().
transaction(Commands) ->
    transaction_with_query_timeout(Commands, ?DEFAULT_QUERY_TIMEOUT).

-spec transaction_with_query_timeout(redis_pipeline_command(), timeout()) -> redis_transaction_result().
transaction_with_query_timeout(Commands, Timeout) ->
    Result = q([["multi"]| Commands] ++ [["exec"]], Timeout),
    lists:last(Result).

%% =============================================================================
%% @doc Execute a function on a pool worker. This function should be use when
%% transaction method such as WATCH or DISCARD must be used. The pool used to
%% execute the transaction is specified by giving a key that this pool is
%% containing.
%% @end
%% =============================================================================
-spec transaction(fun((Worker::pid()) -> redis_result()), anystring()) -> any().
transaction(Transaction, PoolKey) -> transaction(Transaction, PoolKey, ?DEFAULT_QUERY_TIMEOUT).

-spec transaction(fun((Worker::pid()) -> redis_result()), anystring(), timeout()) -> any().
transaction(Transaction, PoolKey, Timeout) ->
    Slot = get_key_slot(PoolKey),
    transaction(Transaction, Slot, undefined, 0, Timeout).

transaction(Transaction, Slot, undefined, _, Timeout) ->
    query2(Transaction, Slot, 0, Timeout);
transaction(Transaction, Slot, ExpectedValue, Counter, Timeout) ->
    case query2(Transaction, Slot, 0, Timeout) of
        ExpectedValue ->
            transaction(Transaction, Slot, ExpectedValue, Counter - 1, Timeout);
        {ExpectedValue, _} ->
            transaction(Transaction, Slot, ExpectedValue, Counter - 1, Timeout);
        Payload ->
            Payload
    end.

%% =============================================================================
%% @doc Multi node query
%% @end
%% =============================================================================
-spec qmn(redis_pipeline_command()) -> redis_pipeline_result().
qmn(Commands) -> qmn(Commands, ?DEFAULT_QUERY_TIMEOUT).

-spec qmn(redis_pipeline_command(), timeout()) -> redis_pipeline_result().
qmn(Commands, Timeout) -> qmn(Commands, 0, Timeout).

qmn(_, ?QUERY_LIMIT, _Timeout) ->
    {error, no_connection};
qmn(Commands, Counter, Timeout) ->
    exponential_backoff(Counter, Timeout),
    {CommandsByPools, MappingInfo} = split_by_pools(Commands),

    case qmn2(CommandsByPools, MappingInfo, [], Timeout) of
        retry -> qmn(Commands, Counter + 1, Timeout);
        Res -> Res
    end.

qmn2([{Pool, PoolCommands} | T1], [{Pool, Mapping} | T2], Acc, Timeout) ->
    Transaction = fun(Worker) -> qw(Worker, PoolCommands, Timeout) end,
    Result = eredis_cluster_pool:transaction(Pool, Transaction),
    case handle_transaction_result(Result, check_pipeline_result) of
        retry -> retry;
        Res ->
            MappedRes = lists:zip(Mapping,Res),
            qmn2(T1, T2, MappedRes ++ Acc, Timeout)
    end;
qmn2([], [], Acc, _Timeout) ->
    SortedAcc =
        lists:sort(
            fun({Index1, _},{Index2, _}) ->
                Index1 < Index2
            end, Acc),
    [Res || {_,Res} <- SortedAcc].

split_by_pools(Commands) ->
    State = eredis_cluster_monitor:get_state(),
    split_by_pools(Commands, 1, [], [], State).

split_by_pools([Command | T], Index, CmdAcc, MapAcc, State) ->
    Key = get_key_from_command(Command),
    Slot = get_key_slot(Key),
    Pool = eredis_cluster_monitor:get_pool_by_slot(Slot, State),
    {NewAcc1, NewAcc2} =
        case lists:keyfind(Pool, 1, CmdAcc) of
            false ->
                {[{Pool, [Command]} | CmdAcc], [{Pool, [Index]} | MapAcc]};
            {Pool, CmdList} ->
                CmdList2 = [Command | CmdList],
                CmdAcc2  = lists:keydelete(Pool, 1, CmdAcc),
                {Pool, MapList} = lists:keyfind(Pool, 1, MapAcc),
                MapList2 = [Index | MapList],
                MapAcc2  = lists:keydelete(Pool, 1, MapAcc),
                {[{Pool, CmdList2} | CmdAcc2], [{Pool, MapList2} | MapAcc2]}
        end,
    split_by_pools(T, Index+1, NewAcc1, NewAcc2, State);
split_by_pools([], _Index, CmdAcc, MapAcc, _State) ->
    CmdAcc2 = [{Pool, lists:reverse(Commands)} || {Pool, Commands} <- CmdAcc],
    MapAcc2 = [{Pool, lists:reverse(Mapping)} || {Pool, Mapping} <- MapAcc],
    {CmdAcc2, MapAcc2}.

%% =============================================================================
%% @doc Wrapper function for command using pipelined commands
%% @end
%% =============================================================================
-spec qp(redis_pipeline_command()) -> redis_pipeline_result().
qp(Commands) -> qp(Commands, ?DEFAULT_QUERY_TIMEOUT).

-spec qp(redis_pipeline_command(), timeout()) -> redis_pipeline_result().
qp(Commands, Timeout) -> q(Commands, Timeout).

%% =============================================================================
%% @doc This function execute simple or pipelined command on a single redis node
%% the node will be automatically found according to the key used in the command
%% @end
%% =============================================================================
-spec q(redis_command()) -> redis_result().
q(Command) ->
    q(Command, ?DEFAULT_QUERY_TIMEOUT).

-spec q(redis_command(), timeout()) -> redis_result().
q(Command, Timeout) ->
    query(Command, Timeout).

-spec qk(redis_command(), bitstring()) -> redis_result().
qk(Command, PoolKey) -> qk(Command, PoolKey, ?DEFAULT_QUERY_TIMEOUT).

-spec qk(redis_command(), bitstring(), timeout()) -> redis_result().
qk(Command, PoolKey, Timeout) ->
    query(Command, PoolKey, Timeout).

-spec query(redis_command(), timeout()) -> redis_result().
query(Command, Timeout) ->
    PoolKey = get_key_from_command(Command),
    query(Command, PoolKey, Timeout).

query(_, undefined, _Timeout) ->
    {error, invalid_cluster_command};
query(Command, PoolKey, Timeout) ->
    Slot = get_key_slot(PoolKey),
    Transaction = fun(Worker) -> qw(Worker, Command, Timeout) end,
    query2(Transaction, Slot, 0, Timeout).

query2(_, _, ?QUERY_LIMIT, _Timeout) ->
    {error, no_connection};
query2(Transaction, Slot, Counter, Timeout) ->
    case exponential_backoff(Counter, Timeout) of
        ok ->
            Pool = eredis_cluster_monitor:get_pool_by_slot(Slot),
            Result = try_query(Pool, Transaction),

            case handle_transaction_result(Result) of
                retry -> query2(Transaction, Slot, Counter + 1, Timeout);
                Payload -> Payload
            end;
        {error, timeout} -> {error, no_connection}
    end.

try_query(Pool, Transaction) ->
    Result = eredis_cluster_pool:transaction(Pool, Transaction),

    case Result of
        % When a redirection is encountered, it is likely multiple slots
        % were reconfigured rather than just one, so updating the client
        % configuration as soon as possible is often the best strategy
        % https://redis.io/topics/cluster-spec
        {error, <<"MOVED ", SlotAddressPort/binary>>} ->
            eredis_cluster_monitor:async_refresh_mapping(),
            try_redirect(SlotAddressPort, Transaction, moved);

        % ASK is like MOVED but the difference is that the client should retry
        % against on new instance only this query, not next queries.
        % When migration is finished, MOVED will be received and only then
        % we should refresh mapping.
        {error, <<"ASK ", SlotAddressPort/binary>>} ->
            try_redirect(SlotAddressPort, Transaction, ask);

        Payload ->
            Payload
    end.

handle_transaction_result(Result) ->
    case Result of
        % If instance down we wait refresh finish and retry
        {error, no_connection} ->
            eredis_cluster_monitor:refresh_mapping(),
            retry;

        % If any other error we retry
        {error, Reason} ->
            log_error(["Redis Cluster Error: ~p", [Reason]]),
            retry;

        % Successful transactions and pool_empty error just return
        Payload -> Payload
    end.
handle_transaction_result(Result, check_pipeline_result) ->
    case handle_transaction_result(Result) of
       retry -> retry;
       Payload when is_list(Payload) ->
           Pred = fun({error, <<"MOVED ", _/binary>>}) -> true;
                     (_) -> false
                  end,
           case lists:any(Pred, Payload) of
               true ->
                   eredis_cluster_monitor:refresh_mapping(),
                   retry;
               false ->
                   Payload
           end;
       Payload -> Payload
    end.

try_redirect(SlotAddressPort, Transaction, RedirectionType) ->
    [Address, Port] = get_address_port(SlotAddressPort),
    case eredis_cluster_pool:create(Address, Port) of
        {ok, Pool} -> redirect_transaction(Pool, Transaction, RedirectionType);
        _ -> retry
    end.

get_address_port(SlotAddressPort) ->
    [_Slot, AddressPort | _] = string:split(binary_to_list(SlotAddressPort), " "),
    [Address, Port] = string:split(AddressPort, ":"),
    [Address, list_to_integer(Port)].

redirect_transaction(Pool, Transaction, moved) ->
    eredis_cluster_pool:transaction(Pool, Transaction);
redirect_transaction(Pool, Transaction, ask) ->
    % ASK redirection: https://redis.io/topics/cluster-spec
    AskTransaction = fun(Worker) -> qw(Worker, ["ASKING"]), Transaction(Worker) end,
    eredis_cluster_pool:transaction(Pool, AskTransaction).


exponential_backoff(0, _Timeout) -> ok;
exponential_backoff(Counter, Timeout) ->
    Time = trunc(math:pow(?EXPONENTIAL_BACKOFF_BASE, Counter)),
    case Timeout < Time of
        true -> timer:sleep(Time);
        false -> {error, timeout}
    end.


%% =============================================================================
%% @doc Update the value of a key by applying the function passed in the
%% argument. The operation is done atomically
%% @end
%% =============================================================================
-spec update_key(Key::anystring(), UpdateFunction::fun((any()) -> any())) ->
    redis_transaction_result().
update_key(Key, UpdateFunction) ->
    UpdateFunction2 = fun(GetResult) ->
        {ok, Var} = GetResult,
        UpdatedVar = UpdateFunction(Var),
        {[["SET", Key, UpdatedVar]], UpdatedVar}
    end,
    case optimistic_locking_transaction(Key, ["GET", Key], UpdateFunction2) of
        {ok, {_, NewValue}} ->
            {ok, NewValue};
        Error ->
            Error
    end.

%% =============================================================================
%% @doc Update the value of a field stored in a hash by applying the function
%% passed in the argument. The operation is done atomically
%% @end
%% =============================================================================
-spec update_hash_field(Key::anystring(), Field::anystring(),
    UpdateFunction::fun((any()) -> any())) -> redis_transaction_result().
update_hash_field(Key, Field, UpdateFunction) ->
    UpdateFunction2 = fun(GetResult) ->
        {ok, Var} = GetResult,
        UpdatedVar = UpdateFunction(Var),
        {[["HSET", Key, Field, UpdatedVar]], UpdatedVar}
    end,
    case optimistic_locking_transaction(Key, ["HGET", Key, Field], UpdateFunction2) of
        {ok, {[FieldPresent], NewValue}} ->
            {ok, {FieldPresent, NewValue}};
        Error ->
            Error
    end.

%% =============================================================================
%% @doc Optimistic locking transaction helper, based on Redis documentation :
%% http://redis.io/topics/transactions
%% @end
%% =============================================================================
-spec optimistic_locking_transaction(Key::anystring(), redis_command(),
    UpdateFunction::fun((redis_result()) -> redis_pipeline_command())) ->
        {redis_transaction_result(), any()}.
optimistic_locking_transaction(WatchedKey, GetCommand, UpdateFunction) ->
    optimistic_locking_transaction(WatchedKey, GetCommand, UpdateFunction, ?DEFAULT_QUERY_TIMEOUT).

-spec optimistic_locking_transaction(Key::anystring(), redis_command(),
    UpdateFunction::fun((redis_result()) -> redis_pipeline_command()), timeout()) ->
        {redis_transaction_result(), any()}.
optimistic_locking_transaction(WatchedKey, GetCommand, UpdateFunction, Timeout) ->
    Slot = get_key_slot(WatchedKey),
    Transaction = fun(Worker) ->
        %% Watch given key
        qw(Worker,["WATCH", WatchedKey], Timeout),
        %% Get necessary information for the modifier function
        GetResult = qw(Worker, GetCommand),
        %% Execute the pipelined command as a redis transaction
        {UpdateCommand, Result} = case UpdateFunction(GetResult) of
            {Command, Var} ->
                {Command, Var};
            Command ->
                {Command, undefined}
        end,
        RedisResult = qw(Worker, [["MULTI"]] ++ UpdateCommand ++ [["EXEC"]], Timeout),
        {lists:last(RedisResult), Result}
    end,
	case transaction(Transaction, Slot, {ok, undefined}, ?OL_TRANSACTION_TTL, Timeout) of
        {{ok, undefined}, _} ->
            {error, resource_busy};
        {{ok, TransactionResult}, UpdateResult} ->
            {ok, {TransactionResult, UpdateResult}};
        {Error, _} ->
            Error
    end.

%% =============================================================================
%% @doc Eval command helper, to optimize the query, it will try to execute the
%% script using its hashed value. If no script is found, it will load it and
%% try again.
%% @end
%% =============================================================================
-spec eval(bitstring(), bitstring(), [bitstring()], [bitstring()]) ->
    redis_result().
eval(Script, ScriptHash, Keys, Args) ->
    eval(Script, ScriptHash, Keys, Args, ?DEFAULT_QUERY_TIMEOUT).

-spec eval(bitstring(), bitstring(), [bitstring()], [bitstring()], timeout()) ->
    redis_result().
eval(Script, ScriptHash, Keys, Args, Timeout) ->
    KeyNb = length(Keys),
    EvalShaCommand = ["EVALSHA", ScriptHash, KeyNb] ++ Keys ++ Args,
    Key = if
        KeyNb == 0 -> "A"; %Random key
        true -> hd(Keys)
    end,

    case qk(EvalShaCommand, Key) of
        {error, <<"NOSCRIPT", _/binary>>} ->
            LoadCommand = ["SCRIPT", "LOAD", Script],
            [_, Result] = qk([LoadCommand, EvalShaCommand], Key, Timeout),
            Result;
        Result ->
            Result
    end.

%% =============================================================================
%% @doc Perform a given query on all node of a redis cluster
%% @end
%% =============================================================================
-spec qa(redis_command()) -> ok | {error, Reason::bitstring()}.
qa(Command) -> qa(Command, ?DEFAULT_QUERY_TIMEOUT).

-spec qa(redis_command(), timeout()) -> ok | {error, Reason::bitstring()}.
qa(Command, Timeout) ->
    Pools = eredis_cluster_monitor:get_all_pools(),
    Transaction = fun(Worker) -> qw(Worker, Command, Timeout) end,
    [eredis_cluster_pool:transaction(Pool, Transaction) || Pool <- Pools].

%% =============================================================================
%% @doc Wrapper function to be used for direct call to a pool worker in the
%% function passed to the transaction/2 method
%% @end
%% =============================================================================
-spec qw(Worker::pid(), redis_command()) -> redis_result().
qw(Worker, Command) ->
    qw(Worker, Command, ?DEFAULT_QUERY_TIMEOUT).

-spec qw(Worker::pid(), redis_command(), timeout()) -> redis_result().
qw(Worker, Command, Timeout) ->
    eredis_cluster_pool_worker:query(Worker, Command, Timeout).

%% =============================================================================
%% @doc Perform flushdb command on each node of the redis cluster
%% @end
%% =============================================================================
-spec flushdb() -> ok | {error, Reason::bitstring()}.
flushdb() ->
    flushdb(?DEFAULT_QUERY_TIMEOUT).

-spec flushdb(timeout()) -> ok | {error, Reason::bitstring()}.
flushdb(Timeout) ->
    Result = qa(["FLUSHDB"], Timeout),
    case proplists:lookup(error,Result) of
        none ->
            ok;
        Error ->
            Error
    end.

%% =============================================================================
%% @doc Return the hash slot from the key
%% @end
%% =============================================================================
-spec get_key_slot(Key::anystring()) -> Slot::integer().
get_key_slot(Key) when is_bitstring(Key) ->
    get_key_slot(bitstring_to_list(Key));
get_key_slot(Key) ->
    KeyToBeHased = case string:chr(Key,${) of
        0 ->
            Key;
        Start ->
            case string:chr(string:substr(Key,Start+1),$}) of
                0 ->
                    Key;
                Length ->
                    if
                        Length =:= 1 ->
                            Key;
                        true ->
                            string:substr(Key,Start+1,Length-1)
                    end
            end
    end,
    eredis_cluster_hash:hash(KeyToBeHased).

%% =============================================================================
%% @doc Return the first key in the command arguments.
%% In a normal query, the second term will be returned
%%
%% If it is a pipeline query we will use the second term of the first term, we
%% will assume that all keys are in the same server and the query can be
%% performed
%%
%% If the pipeline query starts with multi (transaction), we will look at the
%% second term of the second command
%%
%% For eval and evalsha command we will look at the fourth term.
%%
%% For commands that don't make sense in the context of cluster
%% return value will be undefined.
%% @end
%% =============================================================================
-spec get_key_from_command(redis_command()) -> string() | undefined.
get_key_from_command([[X|Y]|Z]) when is_bitstring(X) ->
    get_key_from_command([[bitstring_to_list(X)|Y]|Z]);
get_key_from_command([[X|Y]|Z]) when is_list(X) ->
    case string:to_lower(X) of
        "multi" ->
            get_key_from_command(Z);
        _ ->
            get_key_from_command([X|Y])
    end;
get_key_from_command([Term1,Term2|Rest]) when is_bitstring(Term1) ->
    get_key_from_command([bitstring_to_list(Term1),Term2|Rest]);
get_key_from_command([Term1,Term2|Rest]) when is_bitstring(Term2) ->
    get_key_from_command([Term1,bitstring_to_list(Term2)|Rest]);
get_key_from_command([Term1,Term2|Rest]) ->
    case string:to_lower(Term1) of
        "info" ->
            undefined;
        "config" ->
            undefined;
        "shutdown" ->
            undefined;
        "slaveof" ->
            undefined;
        "eval" ->
            get_key_from_rest(Rest);
        "evalsha" ->
            get_key_from_rest(Rest);
        _ ->
            Term2
    end;
get_key_from_command(_) ->
    undefined.

%% =============================================================================
%% @doc Get key for command where the key is in th 4th position (eval and
%% evalsha commands)
%% @end
%% =============================================================================
-spec get_key_from_rest([anystring()]) -> string() | undefined.
get_key_from_rest([_,KeyName|_]) when is_bitstring(KeyName) ->
    bitstring_to_list(KeyName);
get_key_from_rest([_,KeyName|_]) when is_list(KeyName) ->
    KeyName;
get_key_from_rest(_) ->
    undefined.

-spec log_error([term()]) -> ok.
log_error(Attrs) ->
    case application:get_env(eredis_cluster, logger, true) of
        true -> erlang:apply(logger, error, Attrs);
        false -> ok
    end.
