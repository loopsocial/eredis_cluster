-module(eredis_cluster_monitor).
-behaviour(gen_server).

%% API.
-export([start_link/0]).
-export([connect/1]).
-export([refresh_mapping/0]).
-export([async_refresh_mapping/0]).
-export([get_state/0]).
-export([get_pool_by_slot/1, get_pool_by_slot/2]).
-export([get_all_pools/0]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% Type definition.
-include("eredis_cluster.hrl").
-record(state, {
    init_nodes :: [#node{}],
    slots :: tuple(), %% whose elements are integer indexes into slots_maps
    slots_maps :: tuple(), %% whose elements are #slots_map{}
    refresh_pid :: pid(),
    refresh_callers :: [{pid(),term()}]
}).

%% API.
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

connect(InitServers) ->
    gen_server:call(?MODULE, {connect, InitServers}).

refresh_mapping() ->
    gen_server:call(?MODULE, refresh_mapping, infinity).

async_refresh_mapping() ->
    gen_server:cast(?MODULE, refresh_mapping).

%% =============================================================================
%% @doc Given a slot return the link (Redis instance) to the mapped
%% node.
%% @end
%% =============================================================================

-spec get_state() -> #state{}.
get_state() ->
    case ets:lookup(?MODULE, cluster_state) of
      [{cluster_state, State}] -> State;
      [] -> #state{}
    end.

-spec get_all_pools() -> [pid()].
get_all_pools() ->
    State = get_state(),
    SlotsMapList = tuple_to_list(State#state.slots_maps),
    [SlotsMap#slots_map.node#node.pool || SlotsMap <- SlotsMapList,
        SlotsMap#slots_map.node =/= undefined].

%% =============================================================================
%% @doc Get cluster pool by slot. Optionally, a memoized State can be provided
%% to prevent from querying ets inside loops.
%% @end
%% =============================================================================
-spec get_pool_by_slot(Slot::integer(), State::#state{}) ->
    PoolName::atom() | undefined.
get_pool_by_slot(_Slot, #state{slots = undefined}) ->
    undefined;
get_pool_by_slot(Slot, State) ->
    Index = element(Slot+1,State#state.slots),
    Cluster = element(Index,State#state.slots_maps),
    if
        Cluster#slots_map.node =/= undefined ->
            Cluster#slots_map.node#node.pool;
        true ->
            undefined
    end.

-spec get_pool_by_slot(Slot::integer()) ->
    PoolName::atom() | undefined.
get_pool_by_slot(Slot) ->
    State = get_state(),
    get_pool_by_slot(Slot, State).

%% Private
-spec reload_slots_map(State::#state{}) -> NewState::#state{}.
reload_slots_map(State) ->
    [close_connection(SlotsMap)
        || SlotsMap <- tuple_to_list(State#state.slots_maps)],

    ClusterSlots = get_cluster_slots(State#state.init_nodes),
    SlotsMaps = parse_cluster_slots(ClusterSlots),

    ConnectedSlotsMaps = connect_all_slots(SlotsMaps),
    Slots = create_slots_cache(ConnectedSlotsMaps),

    EtsState = State#state{
        slots = list_to_tuple(Slots),
        slots_maps = list_to_tuple(ConnectedSlotsMaps),
        refresh_callers = []
    },

    true = ets:insert(?MODULE, [{cluster_state, EtsState}]),

    EtsState.

-spec get_cluster_slots([#node{}]) -> [[bitstring() | [bitstring()]]].
get_cluster_slots([]) ->
    throw({error,cannot_connect_to_cluster});
get_cluster_slots([Node|T]) ->
    case safe_eredis_start_link(Node#node.address, Node#node.port) of
        {ok,Connection} ->
          case eredis:q(Connection, ["CLUSTER", "SLOTS"]) of
            {error,<<"ERR unknown command 'CLUSTER'">>} ->
                get_cluster_slots_from_single_node(Node);
            {error,<<"ERR This instance has cluster support disabled">>} ->
                get_cluster_slots_from_single_node(Node);
            {ok, ClusterInfo} ->
                eredis:stop(Connection),
                ClusterInfo;
            _ ->
                eredis:stop(Connection),
                get_cluster_slots(T)
        end;
        _ ->
            get_cluster_slots(T)
  end.

-spec get_cluster_slots_from_single_node(#node{}) ->
    [[bitstring() | [bitstring()]]].
get_cluster_slots_from_single_node(Node) ->
    [[<<"0">>, integer_to_binary(?REDIS_CLUSTER_HASH_SLOTS-1),
    [list_to_binary(Node#node.address), integer_to_binary(Node#node.port)]]].

-spec parse_cluster_slots([[bitstring() | [bitstring()]]]) -> [#slots_map{}].
parse_cluster_slots(ClusterInfo) ->
    parse_cluster_slots(ClusterInfo, 1, []).

parse_cluster_slots([[StartSlot, EndSlot | [[Address, Port | _] | _]] | T], Index, Acc) ->
    SlotsMap =
        #slots_map{
            index = Index,
            start_slot = binary_to_integer(StartSlot),
            end_slot = binary_to_integer(EndSlot),
            node = #node{
                address = binary_to_list(Address),
                port = binary_to_integer(Port)
            }
        },
    parse_cluster_slots(T, Index+1, [SlotsMap | Acc]);
parse_cluster_slots([], _Index, Acc) ->
    lists:reverse(Acc).



-spec close_connection(#slots_map{}) -> ok.
close_connection(SlotsMap) ->
    Node = SlotsMap#slots_map.node,
    if
        Node =/= undefined ->
            try eredis_cluster_pool:stop(Node#node.pool) of
                _ ->
                    ok
            catch
                _ ->
                    ok
            end;
        true ->
            ok
    end.

-spec connect_node(#node{}) -> #node{} | undefined.
connect_node(Node) ->
    case eredis_cluster_pool:create(Node#node.address, Node#node.port) of
        {ok, Pool} ->
            Node#node{pool=Pool};
        _ ->
            undefined
    end.

safe_eredis_start_link(Address,Port) ->
    DataBase = application:get_env(eredis_cluster, database, 0),
    Password = application:get_env(eredis_cluster, password, ""),
    Payload = eredis:start_link(Address, Port, DataBase, Password),
    Payload.

-spec create_slots_cache([#slots_map{}]) -> [integer()].
create_slots_cache(SlotsMaps) ->
  SlotsCache = [[{Index,SlotsMap#slots_map.index}
        || Index <- lists:seq(SlotsMap#slots_map.start_slot,
            SlotsMap#slots_map.end_slot)]
        || SlotsMap <- SlotsMaps],
  SlotsCacheF = lists:flatten(SlotsCache),
  SortedSlotsCache = lists:sort(SlotsCacheF),
  [ Index || {_,Index} <- SortedSlotsCache].

-spec connect_all_slots([#slots_map{}]) -> [integer()].
connect_all_slots(SlotsMapList) ->
    [SlotsMap#slots_map{node=connect_node(SlotsMap#slots_map.node)}
        || SlotsMap <- SlotsMapList].

%% gen_server.
init(_Args) ->
    process_flag(trap_exit, true),
    ets:new(?MODULE, [public, set, named_table, {read_concurrency, true}]),
    InitNodes = application:get_env(eredis_cluster, init_nodes, []),
    {ok, spawn_reload_slots_map(initial_state(InitNodes))}.

handle_call({connect, InitNodes}, _From, _State) ->
    {reply, ok, spawn_reload_slots_map(initial_state(InitNodes))};

handle_call(refresh_mapping, From, #state{refresh_callers = RefreshCallers} = State) ->
    NewState = State#state{refresh_callers = [From|RefreshCallers]},
    % Caller is blocked, response is sent when linked process EXIT
    {noreply, spawn_reload_slots_map(NewState)};

handle_call(get_refresh_callers, _From, State) ->
    % for tests
    {reply, State#state.refresh_callers, State};

handle_call(get_refresh_pid, _From, State) ->
    % for tests
    {reply, State#state.refresh_pid, State};

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(refresh_mapping, State) ->
    {noreply, spawn_reload_slots_map(State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', RefreshPid, Reason}, #state{refresh_pid=RefreshPid} = State) ->
    % Handled when spawn_reload_slots_map exits, either normally or not
    % Cannot update slots here because we cannot assert reload_slots_map succeeded
    {noreply, reply_refresh_callers(State, Reason)};
handle_info({new_mapping, #state{slots = Slots, slots_maps = SlotsMaps}}, State) ->
    % Handled if reload_slots_map succeeds, so we cannot reply callers
    % because we cannot assert spawn_reload_slots_map will exit normal
    {noreply, State#state{slots = Slots, slots_maps = SlotsMaps}};

handle_info(_Info, State) ->
    {noreply, State}.

initial_state(InitNodes) ->
    #state{
        slots = undefined,
        slots_maps = {},
        init_nodes = [#node{address = A, port = P} || {A,P} <- InitNodes],
        refresh_pid = undefined,
        refresh_callers = []
    }.

spawn_reload_slots_map(#state{refresh_pid = undefined} = State) ->
    Me = self(),
    NewMapping = fun () -> Me ! {new_mapping, reload_slots_map(State)} end,
    RefreshPid = proc_lib:spawn_link(NewMapping),
    State#state{refresh_pid = RefreshPid};
spawn_reload_slots_map(#state{refresh_pid = _} = State) ->
    State.

reply_refresh_callers(State, Reason) ->
    Response =
        case Reason == normal of
            true -> {ok, State#state.refresh_pid};
            false -> {error, Reason}
        end,

    [gen_server:reply(From, Response) || From <- State#state.refresh_callers],
    State#state{refresh_pid = undefined, refresh_callers = []}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
