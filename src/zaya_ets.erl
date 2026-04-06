-module(zaya_ets).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  create/1,
  open/1,
  close/1,
  remove/1
]).

%%=================================================================
%%	LOW_LEVEL API
%%=================================================================
-export([
  read/2,
  write/2,
  delete/2
]).

%%=================================================================
%%	ITERATOR API
%%=================================================================
-export([
  first/1,
  last/1,
  next/2,
  prev/2
]).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
-export([
  find/2,
  foldl/4,
  foldr/4
]).

%%=================================================================
%%	COPY API
%%=================================================================
-export([
  copy/3,
  dump_batch/2
]).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
-export([
  commit/3,
  commit1/3,
  commit2/2,
  rollback/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(ref, {
  table,
  pool
}).

-define(none, {?MODULE, undefined}).

%%=================================================================
%%	SERVICE
%%=================================================================
create(Params)->
  open(Params).

open(Params)->
  Table = ets:new(?MODULE,[
    public,
    ordered_set,
    {read_concurrency, true},
    {write_concurrency, true}
  ]),
  PoolOpts = writer_pool_opts(Params),
  try
    {ok, Pool} = zaya_ets_writer_pool:start_link(Table, PoolOpts),
    #ref{
      table = Table,
      pool = Pool
    }
  catch
    Class:Reason:Stack->
      catch ets:delete(Table),
      erlang:raise(Class, Reason, Stack)
  end.

close(#ref{table = Table, pool = Pool})->
  catch zaya_ets_writer_pool:stop(Pool),
  catch ets:delete(Table),
  ok.

remove(_Params)->
  ok.

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(Ref, [Key | Rest])->
  Table = table(Ref),
  case ets:lookup(Table, Key) of
    [Rec]->
      [Rec | read(Ref, Rest)];
    _->
      read(Ref, Rest)
  end;
read(_Ref, [])->
  [].

write(Ref, KVs)->
  zaya_ets_writer_pool:call(pool(Ref), {ops, normalize_write(KVs)}).

delete(Ref, Keys)->
  zaya_ets_writer_pool:call(pool(Ref), {ops, normalize_delete(Keys)}).

%%=================================================================
%%	ITERATOR
%%=================================================================
first(Ref)->
  Table = table(Ref),
  case ets:first_lookup(Table) of
    '$end_of_table'->
      undefined;
    {_First, [Rec]}->
      Rec
  end.

last(Ref)->
  Table = table(Ref),
  case ets:last_lookup(Table) of
    '$end_of_table'->
      undefined;
    {_Last, [Rec]}->
      Rec
  end.

next(Ref, Key)->
  Table = table(Ref),
  case ets:next_lookup(Table, Key) of
    '$end_of_table' ->
      undefined;
    {_Next, [Rec]}->
      Rec
  end.

prev(Ref, Key)->
  Table = table(Ref),
  case ets:prev_lookup(Table, Key) of
    '$end_of_table' ->
      undefined;
    {_Prev, [Rec]}->
      Rec
  end.

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
%----------------------FIND------------------------------------------
find(Ref, Query)->
  Table = table(Ref),
  case {Query, maps:size(Query)} of
    {#{ms := MS}, 1} ->
      ets:select(Table, MS);
    {#{ms := MS, limit := Limit}, 2}->
      case ets:select(Table, MS, Limit) of
        {Result, _Continuation}->
          Result;
        '$end_of_table' ->
          []
      end;
    _->
      First =
        case Query of
          #{start := Start} -> Start;
          _-> ets:first(Table)
        end,
      case Query of
        #{stop := Stop, ms := MS, limit := Limit }->
          CompiledMS = ets:match_spec_compile(MS),
          iterate_query(First, Ref, Stop, CompiledMS, Limit);
        #{stop := Stop, ms := MS }->
          CompiledMS = ets:match_spec_compile(MS),
          iterate_ms_stop(First, Ref, Stop, CompiledMS);
        #{stop := Stop, limit := Limit }->
          iterate_stop_limit(First, Ref, Stop, Limit);
        #{stop := Stop }->
          iterate_stop(First, Ref, Stop);
        #{ms := MS, limit := Limit}->
          CompiledMS = ets:match_spec_compile(MS),
          iterate_ms_limit(First, Ref, CompiledMS, Limit);
        #{ms := MS}->
          CompiledMS = ets:match_spec_compile(MS),
          iterate_ms(First, Ref, CompiledMS);
        _->
          case Query of
            #{start := _}->
              iterate(First, Ref);
            _->
              ets:tab2list(Table)
          end
      end
  end.

iterate_query('$end_of_table', _Ref, _StopKey, _MS, _Limit)->
  [];
iterate_query(Key, Ref, StopKey, MS, Limit) when Key =< StopKey, Limit > 0->
  Table = table(Ref),
  case ets:match_spec_run(ets:lookup(Table, Key), MS) of
    [Res]->
      [Res | iterate_query(ets:next(Table, Key), Ref, StopKey, MS, Limit - 1)];
    []->
      iterate_query(ets:next(Table, Key), Ref, StopKey, MS, Limit)
  end;
iterate_query(_Key, _Ref, _StopKey, _MS, _Limit)->
  [].

iterate_ms_stop('$end_of_table', _Ref, _StopKey, _MS)->
  [];
iterate_ms_stop(Key, Ref, StopKey, MS) when Key =< StopKey->
  Table = table(Ref),
  case ets:match_spec_run(ets:lookup(Table, Key), MS) of
    [Res]->
      [Res | iterate_ms_stop(ets:next(Table, Key), Ref, StopKey, MS)];
    []->
      iterate_ms_stop(ets:next(Table, Key), Ref, StopKey, MS)
  end;
iterate_ms_stop(_Key, _Ref, _StopKey, _MS)->
  [].

iterate_stop_limit('$end_of_table', _Ref, _StopKey, _Limit)->
  [];
iterate_stop_limit(Key, Ref, StopKey, Limit) when Key =< StopKey, Limit > 0->
  Table = table(Ref),
  case ets:lookup(Table, Key) of
    [Res]->
      [Res | iterate_stop_limit(ets:next(Table, Key), Ref, StopKey, Limit - 1)];
    []->
      iterate_stop_limit(ets:next(Table, Key), Ref, StopKey, Limit)
  end;
iterate_stop_limit(_Key, _Ref, _StopKey, _Limit)->
  [].

iterate_stop('$end_of_table', _Ref, _StopKey)->
  [];
iterate_stop(Key, Ref, StopKey) when Key =< StopKey->
  Table = table(Ref),
  case ets:lookup(Table, Key) of
    [Res]->
      [Res | iterate_stop(ets:next(Table, Key), Ref, StopKey)];
    []->
      iterate_stop(ets:next(Table, Key), Ref, StopKey)
  end;
iterate_stop(_Key, _Ref, _StopKey)->
  [].

iterate_ms_limit('$end_of_table', _Ref, _MS, _Limit)->
  [];
iterate_ms_limit(Key, Ref, MS, Limit) when Limit > 0 ->
  Table = table(Ref),
  case ets:match_spec_run(ets:lookup(Table, Key), MS) of
    [Res]->
      [Res | iterate_ms_limit(ets:next(Table, Key), Ref, MS, Limit - 1)];
    []->
      iterate_ms_limit(ets:next(Table, Key), Ref, MS, Limit)
  end;
iterate_ms_limit(_Key, _Ref, _MS, _Limit)->
  [].

iterate_ms('$end_of_table', _Ref, _MS)->
  [];
iterate_ms(Key, Ref, MS)->
  Table = table(Ref),
  case ets:match_spec_run(ets:lookup(Table, Key), MS) of
    [Res]->
      [Res | iterate_ms(ets:next(Table, Key), Ref, MS)];
    []->
      iterate_ms(ets:next(Table, Key), Ref, MS)
  end.

iterate('$end_of_table', _Ref)->
  [];
iterate(Key, Ref)->
  Table = table(Ref),
  case ets:lookup(Table, Key) of
    [Res]->
      [Res | iterate(ets:next(Table, Key), Ref)];
    []->
      iterate(ets:next(Table, Key), Ref)
  end.

%----------------------FOLD LEFT------------------------------------------
foldl(Ref, Query, UserFun, InAcc)->
  Table = table(Ref),
  First =
    case Query of
      #{start := Start}-> Start;
      _-> ets:first(Table)
    end,
  Fun =
    case Query of
      #{ms := MS}->
        CompiledMS = ets:match_spec_compile(MS),
        fun(Rec, Acc)->
          case ets:match_spec_run([Rec], CompiledMS) of
            [Res]->
              UserFun(Res, Acc);
            []->
              Acc
          end
        end;
      _->
        UserFun
    end,

  try
    case Query of
      #{stop := Stop }->
        do_foldl_stop(First, Ref, Fun, InAcc, Stop);
      _->
        do_foldl(First, Ref, Fun, InAcc)
    end
  catch
    {stop, Acc}->Acc
  end.

do_foldl_stop('$end_of_table', _Ref, _Fun, Acc, _StopKey)->
  Acc;
do_foldl_stop(Key, Ref, Fun, InAcc, StopKey) when Key =< StopKey->
  Table = table(Ref),
  case ets:lookup(Table, Key) of
    [Rec]->
      Acc = Fun(Rec, InAcc),
      do_foldl_stop(ets:next(Table, Key), Ref, Fun, Acc, StopKey);
    []->
      do_foldl_stop(ets:next(Table, Key), Ref, Fun, InAcc, StopKey)
  end;
do_foldl_stop(_Key, _Ref, _Fun, Acc, _StopKey)->
  Acc.

do_foldl('$end_of_table', _Ref, _Fun, Acc)->
  Acc;
do_foldl(Key, Ref, Fun, InAcc)->
  Table = table(Ref),
  case ets:lookup(Table, Key) of
    [Rec]->
      Acc = Fun(Rec, InAcc),
      do_foldl(ets:next(Table, Key), Ref, Fun, Acc);
    []->
      do_foldl(ets:next(Table, Key), Ref, Fun, InAcc)
  end.

%----------------------FOLD RIGHT------------------------------------------
foldr(Ref, Query, UserFun, InAcc)->
  Table = table(Ref),
  Last =
    case Query of
      #{start := Start}-> Start;
      _-> ets:last(Table)
    end,
  Fun =
    case Query of
      #{ms := MS}->
        CompiledMS = ets:match_spec_compile(MS),
        fun(Rec, Acc)->
          case ets:match_spec_run([Rec], CompiledMS) of
            [Res]->
              UserFun(Res, Acc);
            []->
              Acc
          end
        end;
      _->
        UserFun
    end,

  try
    case Query of
      #{stop := Stop }->
        do_foldr_stop(Last, Ref, Fun, InAcc, Stop);
      _->
        do_foldr(Last, Ref, Fun, InAcc)
    end
  catch
    {stop, Acc}-> Acc
  end.

do_foldr_stop('$end_of_table', _Ref, _Fun, Acc, _StopKey)->
  Acc;
do_foldr_stop(Key, Ref, Fun, InAcc, StopKey) when Key >= StopKey->
  Table = table(Ref),
  case ets:lookup(Table, Key) of
    [Rec]->
      Acc = Fun(Rec, InAcc),
      do_foldr_stop(ets:prev(Table, Key), Ref, Fun, Acc, StopKey);
    []->
      do_foldr_stop(ets:prev(Table, Key), Ref, Fun, InAcc, StopKey)
  end;
do_foldr_stop(_Key, _Ref, _Fun, Acc, _StopKey)->
  Acc.

do_foldr('$end_of_table', _Ref, _Fun, Acc)->
  Acc;
do_foldr(Key, Ref, Fun, InAcc)->
  Table = table(Ref),
  case ets:lookup(Table, Key) of
    [Rec]->
      Acc = Fun(Rec, InAcc),
      do_foldr(ets:prev(Table, Key), Ref, Fun, Acc);
    []->
      do_foldr(ets:prev(Table, Key), Ref, Fun, InAcc)
  end.

%%=================================================================
%%	COPY
%%=================================================================
copy(Ref, Fun, InAcc)->
  foldl(Ref, #{}, Fun, InAcc).

dump_batch(Ref, KVs)->
  do_dump_batch(table(Ref), KVs).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
commit(Ref, Write, Delete)->
  zaya_ets_writer_pool:call(pool(Ref), {ops, normalize_commit(Write, Delete)}).

commit1(_Ref, Write, Delete)->
  {Write, Delete}.

commit2(Ref, {Write, Delete})->
  zaya_ets_writer_pool:call(pool(Ref), {ops, normalize_commit(Write, Delete)}).

rollback(_Ref, _TRef)->
  ok.

%%=================================================================
%%	INFO
%%=================================================================
get_size(Ref)->
  erlang:system_info(wordsize) * ets:info(table(Ref), memory).

%%=================================================================
%%	INTERNAL
%%=================================================================
table(#ref{table = Table})->
  Table.

pool(#ref{pool = Pool})->
  Pool.

normalize_write(KVs)->
  [{put, Key, Value} || {Key, Value} <- KVs].

normalize_delete(Keys)->
  [{delete, Key} || Key <- Keys].

normalize_commit(Write, Delete)->
  normalize_write(Write) ++ normalize_delete(Delete).

do_dump_batch(_Table, [])->
  ok;
do_dump_batch(Table, KVs)->
  true = ets:insert(Table, KVs),
  ok.

writer_pool_opts(Params) when is_map(Params)->
  maps:merge(
    #{
      size => default_writer_pool_size(),
      batch_size => 1000
    },
    maps:get(writer_pool, Params, #{})
  ).

default_writer_pool_size()->
  case erlang:system_info(logical_processors) of
    unknown ->
      1;
    Size when is_integer(Size), Size > 0 ->
      Size
  end.
