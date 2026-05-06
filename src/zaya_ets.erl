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
  prepare_rollback/3,
  is_persistent/0
]).

%%=================================================================
%%	POOL API
%%=================================================================
-export([
  pool_batch/2
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
  try
    Pool = open_pool(Table, Params),
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
  catch close_pool(Pool),
  catch ets:delete(Table),
  ok.

remove(_Params)->
  ok.

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(#ref{table = Table}, Keys)->
  do_read(Table, Keys).
do_read(Table, [Key|Rest])->
  case ets:lookup(Table, Key) of
    [Rec]->
      [Rec | do_read(Table, Rest)];
    _->
      do_read(Table, Rest)
  end;
do_read(_Ref, [])->
  [].

write(#ref{ table = Table, pool = disabled }, KVs)->
  ets:insert(Table, KVs),
  ok;
write(#ref{pool = Pool}, KVs)->
  Writes = [{write, KVs}],
  zaya_pool:call(Pool, Writes).

delete(#ref{table = Table, pool = disabled}, Keys)->
  [ets:delete(Table, K) || K <- Keys],
  ok;
delete(#ref{pool = Pool}, Keys)->
  Deletes = [{delete, Keys}],
  zaya_pool:call(Pool, Deletes).

%%=================================================================
%%	ITERATOR
%%=================================================================
first(#ref{table = Table})->
  case ets:first_lookup(Table) of
    '$end_of_table'->
      undefined;
    {_First, [Rec]}->
      Rec
  end.

last(#ref{table = Table})->
  case ets:last_lookup(Table) of
    '$end_of_table'->
      undefined;
    {_Last, [Rec]}->
      Rec
  end.

next(#ref{table = Table}, Key)->
  case ets:next_lookup(Table, Key) of
    '$end_of_table' ->
      undefined;
    {_Next, [Rec]}->
      Rec
  end.

prev(#ref{table = Table}, Key)->
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
find(#ref{table = Table}, Query)->
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
      First = forward_start_lookup(Table, Query),
      case Query of
        #{stop := Stop, ms := MS, limit := Limit }->
          CompiledMS = ets:match_spec_compile(MS),
          iterate_query(First, Table, Stop, CompiledMS, Limit);
        #{stop := Stop, ms := MS }->
          CompiledMS = ets:match_spec_compile(MS),
          iterate_ms_stop(First, Table, Stop, CompiledMS);
        #{stop := Stop, limit := Limit }->
          iterate_stop_limit(First, Table, Stop, Limit);
        #{stop := Stop }->
          iterate_stop(First, Table, Stop);
        #{ms := MS, limit := Limit}->
          CompiledMS = ets:match_spec_compile(MS),
          iterate_ms_limit(First, Table, CompiledMS, Limit);
        #{ms := MS}->
          CompiledMS = ets:match_spec_compile(MS),
          iterate_ms(First, Table, CompiledMS);
        _->
          case Query of
            #{start := _}->
              iterate(First, Table);
            _->
              ets:tab2list(Table)
          end
      end
  end.

iterate_query('$end_of_table', _Table, _StopKey, _MS, _Limit)->
  [];
iterate_query({Key, Objects}, Table, StopKey, MS, Limit) when Key =< StopKey, Limit > 0->
  case ets:match_spec_run(Objects, MS) of
    [Res]->
      [Res | iterate_query(ets:next_lookup(Table, Key), Table, StopKey, MS, Limit - 1)];
    []->
      iterate_query(ets:next_lookup(Table, Key), Table, StopKey, MS, Limit)
  end;
iterate_query(_Key, _Table, _StopKey, _MS, _Limit)->
  [].

iterate_ms_stop('$end_of_table', _Table, _StopKey, _MS)->
  [];
iterate_ms_stop({Key, Objects}, Table, StopKey, MS) when Key =< StopKey->
  case ets:match_spec_run(Objects, MS) of
    [Res]->
      [Res | iterate_ms_stop(ets:next_lookup(Table, Key), Table, StopKey, MS)];
    []->
      iterate_ms_stop(ets:next_lookup(Table, Key), Table, StopKey, MS)
  end;
iterate_ms_stop(_Key, _Table, _StopKey, _MS)->
  [].

iterate_stop_limit('$end_of_table', _Table, _StopKey, _Limit)->
  [];
iterate_stop_limit({Key, [Res]}, Table, StopKey, Limit) when Key =< StopKey, Limit > 0->
  [Res | iterate_stop_limit(ets:next_lookup(Table, Key), Table, StopKey, Limit - 1)];
iterate_stop_limit(_Key, _Table, _StopKey, _Limit)->
  [].

iterate_stop('$end_of_table', _Table, _StopKey)->
  [];
iterate_stop({Key, [Res]}, Table, StopKey) when Key =< StopKey->
  [Res | iterate_stop(ets:next_lookup(Table, Key), Table, StopKey)];
iterate_stop(_Key, _Table, _StopKey)->
  [].

iterate_ms_limit('$end_of_table', _Table, _MS, _Limit)->
  [];
iterate_ms_limit({Key, Objects}, Table, MS, Limit) when Limit > 0 ->
  case ets:match_spec_run(Objects, MS) of
    [Res]->
      [Res | iterate_ms_limit(ets:next_lookup(Table, Key), Table, MS, Limit - 1)];
    []->
      iterate_ms_limit(ets:next_lookup(Table, Key), Table, MS, Limit)
  end;
iterate_ms_limit(_Key, _Table, _MS, _Limit)->
  [].

iterate_ms('$end_of_table', _Table, _MS)->
  [];
iterate_ms({Key, Objects}, Table, MS)->
  case ets:match_spec_run(Objects, MS) of
    [Res]->
      [Res | iterate_ms(ets:next_lookup(Table, Key), Table, MS)];
    []->
      iterate_ms(ets:next_lookup(Table, Key), Table, MS)
  end.

iterate('$end_of_table', _Table)->
  [];
iterate({Key, [Res]}, Table)->
  [Res | iterate(ets:next_lookup(Table, Key), Table)].

%----------------------FOLD LEFT------------------------------------------
foldl(#ref{table = Table}, Query, UserFun, InAcc)->
  First = forward_start_lookup(Table, Query),
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
        do_foldl_stop(First, Table, Fun, InAcc, Stop);
      _->
        do_foldl(First, Table, Fun, InAcc)
    end
  catch
    {stop, Acc}->Acc
  end.

do_foldl_stop('$end_of_table', _Table, _Fun, Acc, _StopKey)->
  Acc;
do_foldl_stop({Key, [Rec]}, Table, Fun, InAcc, StopKey) when Key =< StopKey->
  Acc = Fun(Rec, InAcc),
  do_foldl_stop(ets:next_lookup(Table, Key), Table, Fun, Acc, StopKey);
do_foldl_stop(_Key, _Table, _Fun, Acc, _StopKey)->
  Acc.

do_foldl('$end_of_table', _Table, _Fun, Acc)->
  Acc;
do_foldl({Key, [Rec]}, Table, Fun, InAcc)->
  Acc = Fun(Rec, InAcc),
  do_foldl(ets:next_lookup(Table, Key), Table, Fun, Acc).

%----------------------FOLD RIGHT------------------------------------------
foldr(#ref{table = Table}, Query, UserFun, InAcc)->
  Last = backward_start_lookup(Table, Query),
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
        do_foldr_stop(Last, Table, Fun, InAcc, Stop);
      _->
        do_foldr(Last, Table, Fun, InAcc)
    end
  catch
    {stop, Acc}-> Acc
  end.

do_foldr_stop('$end_of_table', _Table, _Fun, Acc, _StopKey)->
  Acc;
do_foldr_stop({Key, [Rec]}, Table, Fun, InAcc, StopKey) when Key >= StopKey->
  Acc = Fun(Rec, InAcc),
  do_foldr_stop(ets:prev_lookup(Table, Key), Table, Fun, Acc, StopKey);
do_foldr_stop(_Key, _Table, _Fun, Acc, _StopKey)->
  Acc.

do_foldr('$end_of_table', _Table, _Fun, Acc)->
  Acc;
do_foldr({Key, [Rec]}, Table, Fun, InAcc)->
  Acc = Fun(Rec, InAcc),
  do_foldr(ets:prev_lookup(Table, Key), Table, Fun, Acc).

%%=================================================================
%%	COPY
%%=================================================================
copy(Ref, Fun, InAcc)->
  foldl(Ref, #{}, Fun, InAcc).

dump_batch(#ref{table = Table}, KVs)->
  true = ets:insert(Table, KVs),
  ok.

%%=================================================================
%%	TRANSACTION API
%%=================================================================
commit(#ref{table = Table, pool = disabled}, Write, Delete)->
  ets:insert(Table, Write),
  [ets:delete(Table, K) || K <- Delete],
  ok;
commit(#ref{pool = Pool}, Write, Delete)->
  Commits = [{write,Write}, {delete, Delete}],
  zaya_pool:call(Pool, Commits).

prepare_rollback(#ref{table = Table}, Write, Delete)->
  {W_acc0, D_acc} = rollback_write(Write, Table, {[],[]}),
  W_acc = rollback_delete(Delete, Table, W_acc0),
  {W_acc, D_acc}.

rollback_write([{K,V}|Rest], Table, Acc0 = {W_acc,D_acc})->
  Acc =
    case ets:lookup(Table, K) of
      [{K,V}]-> Acc0;
      [Rec0] -> {[Rec0|W_acc], D_acc};
      _-> {W_acc, [K|D_acc]}
    end,
  rollback_write(Rest, Table, Acc);
rollback_write([], _Table, Acc)->
  Acc.

rollback_delete([K|Rest], Table, Acc0)->
  Acc =
    case ets:lookup(Table, K) of
      [Rec] -> [Rec|Acc0];
      _-> Acc0
    end,
  rollback_delete(Rest, Table, Acc);
rollback_delete([], _Table, Acc)->
  Acc.

is_persistent()->
  false.

%%=================================================================
%%	POOL API
%%=================================================================
pool_batch(Table, Requests)->
  pool_batch(Requests, Table, _Writes = []).
pool_batch([{write, KVs}|Rest], Table, Writes)->
  pool_batch(Rest, Table, [KVs|Writes]);
pool_batch(Requests, Table, [_|_]=Writes)->
  KVs = lists:append(lists:reverse(Writes)),
  ets:insert(Table, KVs),
  pool_batch(Requests, Table, []);
pool_batch([{delete, Keys}|Rest], Table, Writes)->
  [ets:delete(Table, K) || K <- Keys],
  pool_batch(Rest, Table, Writes);
pool_batch([], _Table, [])->
  ok.

%%=================================================================
%%	INFO
%%=================================================================
get_size(#ref{table = Table})->
  erlang:system_info(wordsize) * ets:info(Table, memory).

%%=================================================================
%%	INTERNAL UTILITIES
%%=================================================================
open_pool(_Table, #{pool := disabled})->
  disabled;
open_pool(Table, Params) when is_map(Params)->
  {ok, Pool} = zaya_pool:start_link(pool_params(Table, Params)),
  Pool.

close_pool(disabled)->
  ok;
close_pool(Pool)->
  zaya_pool:stop(Pool).

pool_params(Table, Params) when is_map(Params)->
  maps:merge(
    maps:get(pool, Params, #{}),
    #{
      ref => Table,
      module => ?MODULE
    }
  ).

forward_start_lookup(Table, #{start := Start})->
  current_or_next_lookup(Table, Start);
forward_start_lookup(Table, _Query)->
  ets:first_lookup(Table).

backward_start_lookup(Table, #{start := Start})->
  current_or_prev_lookup(Table, Start);
backward_start_lookup(Table, _Query)->
  ets:last_lookup(Table).

current_or_next_lookup(Table, Key)->
  case ets:lookup(Table, Key) of
    [Rec]->
      {Key, [Rec]};
    []->
      ets:next_lookup(Table, Key)
  end.

current_or_prev_lookup(Table, Key)->
  case ets:lookup(Table, Key) of
    [Rec]->
      {Key, [Rec]};
    []->
      ets:prev_lookup(Table, Key)
  end.
