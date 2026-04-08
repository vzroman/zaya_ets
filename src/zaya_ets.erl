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
  pool_batch/2,
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
pool_batch(Table, Requests)->
  ok = apply_pool_requests(Table, Requests).

read(#ref{table = Table} = Ref, [Key | Rest])->
  case ets:lookup(Table, Key) of
    [Rec]->
      [Rec | read(Ref, Rest)];
    _->
      read(Ref, Rest)
  end;
read(_Ref, [])->
  [].

write(Ref, KVs)->
  Writes = [{write, KVs}],
  call_pool(Ref, Writes).

delete(Ref, Keys)->
  Deletes = [{delete, Keys}],
  call_pool(Ref, Deletes).

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
      First =
        case Query of
          #{start := Start} -> Start;
          _-> ets:first(Table)
        end,
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
iterate_query(Key, Table, StopKey, MS, Limit) when Key =< StopKey, Limit > 0->
  case ets:match_spec_run(ets:lookup(Table, Key), MS) of
    [Res]->
      [Res | iterate_query(ets:next(Table, Key), Table, StopKey, MS, Limit - 1)];
    []->
      iterate_query(ets:next(Table, Key), Table, StopKey, MS, Limit)
  end;
iterate_query(_Key, _Table, _StopKey, _MS, _Limit)->
  [].

iterate_ms_stop('$end_of_table', _Table, _StopKey, _MS)->
  [];
iterate_ms_stop(Key, Table, StopKey, MS) when Key =< StopKey->
  case ets:match_spec_run(ets:lookup(Table, Key), MS) of
    [Res]->
      [Res | iterate_ms_stop(ets:next(Table, Key), Table, StopKey, MS)];
    []->
      iterate_ms_stop(ets:next(Table, Key), Table, StopKey, MS)
  end;
iterate_ms_stop(_Key, _Table, _StopKey, _MS)->
  [].

iterate_stop_limit('$end_of_table', _Table, _StopKey, _Limit)->
  [];
iterate_stop_limit(Key, Table, StopKey, Limit) when Key =< StopKey, Limit > 0->
  case ets:lookup(Table, Key) of
    [Res]->
      [Res | iterate_stop_limit(ets:next(Table, Key), Table, StopKey, Limit - 1)];
    []->
      iterate_stop_limit(ets:next(Table, Key), Table, StopKey, Limit)
  end;
iterate_stop_limit(_Key, _Table, _StopKey, _Limit)->
  [].

iterate_stop('$end_of_table', _Table, _StopKey)->
  [];
iterate_stop(Key, Table, StopKey) when Key =< StopKey->
  case ets:lookup(Table, Key) of
    [Res]->
      [Res | iterate_stop(ets:next(Table, Key), Table, StopKey)];
    []->
      iterate_stop(ets:next(Table, Key), Table, StopKey)
  end;
iterate_stop(_Key, _Table, _StopKey)->
  [].

iterate_ms_limit('$end_of_table', _Table, _MS, _Limit)->
  [];
iterate_ms_limit(Key, Table, MS, Limit) when Limit > 0 ->
  case ets:match_spec_run(ets:lookup(Table, Key), MS) of
    [Res]->
      [Res | iterate_ms_limit(ets:next(Table, Key), Table, MS, Limit - 1)];
    []->
      iterate_ms_limit(ets:next(Table, Key), Table, MS, Limit)
  end;
iterate_ms_limit(_Key, _Table, _MS, _Limit)->
  [].

iterate_ms('$end_of_table', _Table, _MS)->
  [];
iterate_ms(Key, Table, MS)->
  case ets:match_spec_run(ets:lookup(Table, Key), MS) of
    [Res]->
      [Res | iterate_ms(ets:next(Table, Key), Table, MS)];
    []->
      iterate_ms(ets:next(Table, Key), Table, MS)
  end.

iterate('$end_of_table', _Table)->
  [];
iterate(Key, Table)->
  case ets:lookup(Table, Key) of
    [Res]->
      [Res | iterate(ets:next(Table, Key), Table)];
    []->
      iterate(ets:next(Table, Key), Table)
  end.

%----------------------FOLD LEFT------------------------------------------
foldl(#ref{table = Table}, Query, UserFun, InAcc)->
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
        do_foldl_stop(First, Table, Fun, InAcc, Stop);
      _->
        do_foldl(First, Table, Fun, InAcc)
    end
  catch
    {stop, Acc}->Acc
  end.

do_foldl_stop('$end_of_table', _Table, _Fun, Acc, _StopKey)->
  Acc;
do_foldl_stop(Key, Table, Fun, InAcc, StopKey) when Key =< StopKey->
  case ets:lookup(Table, Key) of
    [Rec]->
      Acc = Fun(Rec, InAcc),
      do_foldl_stop(ets:next(Table, Key), Table, Fun, Acc, StopKey);
    []->
      do_foldl_stop(ets:next(Table, Key), Table, Fun, InAcc, StopKey)
  end;
do_foldl_stop(_Key, _Table, _Fun, Acc, _StopKey)->
  Acc.

do_foldl('$end_of_table', _Table, _Fun, Acc)->
  Acc;
do_foldl(Key, Table, Fun, InAcc)->
  case ets:lookup(Table, Key) of
    [Rec]->
      Acc = Fun(Rec, InAcc),
      do_foldl(ets:next(Table, Key), Table, Fun, Acc);
    []->
      do_foldl(ets:next(Table, Key), Table, Fun, InAcc)
  end.

%----------------------FOLD RIGHT------------------------------------------
foldr(#ref{table = Table}, Query, UserFun, InAcc)->
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
        do_foldr_stop(Last, Table, Fun, InAcc, Stop);
      _->
        do_foldr(Last, Table, Fun, InAcc)
    end
  catch
    {stop, Acc}-> Acc
  end.

do_foldr_stop('$end_of_table', _Table, _Fun, Acc, _StopKey)->
  Acc;
do_foldr_stop(Key, Table, Fun, InAcc, StopKey) when Key >= StopKey->
  case ets:lookup(Table, Key) of
    [Rec]->
      Acc = Fun(Rec, InAcc),
      do_foldr_stop(ets:prev(Table, Key), Table, Fun, Acc, StopKey);
    []->
      do_foldr_stop(ets:prev(Table, Key), Table, Fun, InAcc, StopKey)
  end;
do_foldr_stop(_Key, _Table, _Fun, Acc, _StopKey)->
  Acc.

do_foldr('$end_of_table', _Table, _Fun, Acc)->
  Acc;
do_foldr(Key, Table, Fun, InAcc)->
  case ets:lookup(Table, Key) of
    [Rec]->
      Acc = Fun(Rec, InAcc),
      do_foldr(ets:prev(Table, Key), Table, Fun, Acc);
    []->
      do_foldr(ets:prev(Table, Key), Table, Fun, InAcc)
  end.

%%=================================================================
%%	COPY
%%=================================================================
copy(Ref, Fun, InAcc)->
  foldl(Ref, #{}, Fun, InAcc).

dump_batch(#ref{table = Table}, KVs)->
  do_dump_batch(Table, KVs).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
commit(Ref, Write, Delete)->
  Commits = [{write,Write}, {delete, Delete}],
  call_pool(Ref, Commits).

commit1(_Ref, Write, Delete)->
  {Write, Delete}.

commit2(Ref, {Write, Delete})->
  Commits = [{write,Write}, {delete, Delete}],
  call_pool(Ref, Commits).

rollback(_Ref, _TRef)->
  ok.

%%=================================================================
%%	INFO
%%=================================================================
get_size(#ref{table = Table})->
  erlang:system_info(wordsize) * ets:info(Table, memory).

%%=================================================================
%%	INTERNAL
%%=================================================================

do_dump_batch(_Table, [])->
  ok;
do_dump_batch(Table, KVs)->
  true = ets:insert(Table, KVs),
  ok.

pool_params(Table, Params) when is_map(Params)->
  maps:merge(
    #{
      ref => Table,
      module => ?MODULE
    },
    maps:get(pool, Params, #{})
  ).

open_pool(_Table, #{pool := disabled})->
  disabled;
open_pool(Table, Params) when is_map(Params)->
  {ok, Pool} = zaya_pool:start_link(pool_params(Table, Params)),
  Pool.

close_pool(disabled)->
  ok;
close_pool(Pool)->
  zaya_pool:stop(Pool).

call_pool(#ref{table = Table, pool = disabled}, Requests)->
  pool_batch(Table, Requests);
call_pool(#ref{pool = Pool}, Requests)->
  zaya_pool:call(Pool, Requests).

apply_pool_requests(_Table, [])->
  ok;
apply_pool_requests(Table, [{write, KVs} | Rest])->
  true = ets:insert(Table, KVs),
  apply_pool_requests(Table, Rest);
apply_pool_requests(Table, [{delete, Keys} | Rest])->
  ok = delete_keys(Table, Keys),
  apply_pool_requests(Table, Rest).

delete_keys(_Table, [])->
  ok;
delete_keys(Table, [Key | Rest])->
  true = ets:delete(Table, Key),
  delete_keys(Table, Rest).
