
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
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(ref,{ ets, pool }).
%%=================================================================
%%	SERVICE
%%=================================================================
create( Params )->
  open( Params ).

open( _Params )->
  Ets = ets:new(?MODULE,[
    public,
    ordered_set,
    {read_concurrency, true},
    {write_concurrency, auto}
  ]),

  Pool = list_to_tuple([ spawn_link(fun()->loop( Ets) end) || _ <- lists:seq(1,8)]),
  #ref{ ets = Ets, pool = Pool }.

close( #ref{ets = Ets, pool = Pool} )->
  [ exit(P, shutdown) || P <- tuple_to_list(Pool) ],
  catch ets:delete( Ets ),
  ok.

remove( _Params )->
  ok.


-define(TRY( Exp ), try {ok,self(),Exp} catch _:E-> {error,self(),E} end).
loop( Ets )->
  receive
    {read, PID, Keys}->
      PID ! ?TRY( do_read( Ets, Keys ));
    {write, PID, KVs}->
      PID ! ?TRY( do_write( Ets, KVs ));
    {delete, PID, Keys}->
      PID ! ?TRY( do_delete( Ets, Keys ));
    {first, PID}->
      PID ! ?TRY( do_first( Ets ));
    {last, PID}->
      PID ! ?TRY( do_last( Ets ));
    {next, PID, Key}->
      PID ! ?TRY( do_next( Ets, Key ));
    {prev, PID, Key}->
      PID ! ?TRY( do_prev( Ets, Key ));
    _->
      ignore
  end,

  loop( Ets ).

-define(pid(P), element(erlang:phash2(make_ref(), 8) + 1, Pool)).
-define(result(P),
  receive
    {ok,P,_@Res} -> _@Res;
    {error,P,_@Err} -> throw( _@Err )
  end).

read(#ref{pool = Pool}, Keys)->
  PID = ?pid( Pool ),
  PID ! { read, self(), Keys },
  ?result( PID ).

write(#ref{pool = Pool}, KVs)->
  PID = ?pid( Pool ),
  PID ! { write, self(), KVs },
  ?result( PID ).

delete(#ref{pool = Pool}, Keys)->
  PID = ?pid( Pool ),
  PID ! { delete, self(), Keys },
  ?result( PID ).

first(#ref{pool = Pool})->
  PID = ?pid( Pool ),
  PID ! { first, self() },
  ?result( PID ).

last(#ref{pool = Pool})->
  PID = ?pid( Pool ),
  PID ! { last, self() },
  ?result( PID ).

next(#ref{pool = Pool}, Key)->
  PID = ?pid( Pool ),
  PID ! { next, self(), Key },
  ?result( PID ).

prev(#ref{pool = Pool}, Key)->
  PID = ?pid( Pool ),
  PID ! { prev, self(), Key },
  ?result( PID ).

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
do_read(Ref, [Key|Rest])->
  case ets:lookup(Ref,Key) of
    [Rec]->
      [Rec | do_read(Ref, Rest)];
    _->
      do_read(Ref, Rest)
  end;
do_read(_Ref,[])->
  [].

do_write(Ref, KVs)->
  ets:insert( Ref, KVs ),
  ok.

do_delete(Ref,Keys)->
  [ ets:delete(Ref, K) || K <- Keys],
  ok.


%%=================================================================
%%	ITERATOR
%%=================================================================
do_first( Ref )->
  case ets:first( Ref ) of
    '$end_of_table'-> throw( undefined );
    Key->
      case ets:lookup(Ref, Key ) of
        [Rec]->
          Rec;
        _->
          do_next( Ref, Key )
      end
  end.

do_last( Ref )->
  case ets:last( Ref ) of
    '$end_of_table'-> throw( undefined );
    Key->
      case ets:lookup(Ref, Key ) of
        [Rec]->
          Rec;
        _->
          do_prev( Ref, Key )
      end
  end.

do_next( Ref, Key )->
  case ets:next( Ref, Key ) of
    '$end_of_table' -> throw( undefined );
    Next->
      case ets:lookup( Ref, Next ) of
        [Rec]-> Rec;
        _-> do_next( Ref, Next )
      end
  end.

do_prev( Ref, Key )->
  case ets:prev( Ref, Key ) of
    '$end_of_table' -> throw( undefined );
    Prev->
      case ets:lookup( Ref, Prev ) of
        [Rec]-> Rec;
        _-> do_prev( Ref, Prev )
      end
  end.

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
%----------------------FIND------------------------------------------
find(#ref{ ets = Ref }, Query)->
  case {Query, maps:size( Query )} of
    { #{ ms := MS }, 1} ->
      ets:select(Ref, MS);
    { #{ms := MS, limit := Limit}, 2}->
      case ets:select(Ref, MS, Limit) of
        {Result, _Continuation}->
          Result;
        '$end_of_table' ->
          throw( undefined )
      end;
    _->
      First =
        case Query of
          #{ start := Start} -> Start;
          _-> ets:first( Ref )
        end,
        case Query of
          #{  stop := Stop, ms := MS, limit := Limit }->
            CompiledMS = ets:match_spec_compile(MS),
            iterate_query(First, Ref, Stop, CompiledMS, Limit  );
          #{  stop := Stop, ms := MS }->
            CompiledMS = ets:match_spec_compile(MS),
            iterate_ms_stop(First, Ref, Stop, CompiledMS );
          #{ stop:= Stop, limit := Limit }->
            iterate_stop_limit(First, Ref, Stop, Limit );
          #{ stop:= Stop }->
            iterate_stop(First, Ref, Stop );
          #{ms := MS, limit := Limit}->
            CompiledMS = ets:match_spec_compile(MS),
            iterate_ms_limit(First, Ref, CompiledMS, Limit );
          #{ms := MS}->
            CompiledMS = ets:match_spec_compile(MS),
            iterate_ms(First, Ref, CompiledMS );
          _->
            case Query of
              #{start:=_}->
                iterate( First, Ref );
              _->
                ets:tab2list( Ref )
            end
        end
  end.

iterate_query('$end_of_table', _Ref, _StopKey, _MS, _Limit  )->
  [];
iterate_query(Key, Ref, StopKey, MS, Limit  ) when Key =< StopKey, Limit > 0->
  case ets:match_spec_run(ets:lookup(Ref, Key ), MS) of
    [Res]->
      [Res | iterate_query(ets:next(Ref,Key), Ref, StopKey, MS, Limit -1 )];
    []->
      iterate_query(ets:next(Ref,Key), Ref, StopKey, MS, Limit  )
  end;
iterate_query(_Key, _Ref, _StopKey, _MS, _Limit  )->
  [].

iterate_ms_stop('$end_of_table', _Ref, _StopKey, _MS )->
  [];
iterate_ms_stop(Key, Ref, StopKey, MS ) when Key =< StopKey->
  case ets:match_spec_run(ets:lookup(Ref, Key ), MS) of
    [Res]->
      [Res | iterate_ms_stop(ets:next(Ref,Key), Ref, StopKey, MS )];
    []->
      iterate_ms_stop(ets:next(Ref,Key), Ref, StopKey, MS )
  end;
iterate_ms_stop(_Key, _Ref, _StopKey, _MS )->
  [].

iterate_stop_limit('$end_of_table', _Ref, _StopKey, _Limit )->
  [];
iterate_stop_limit(Key, Ref, StopKey, Limit ) when Key =< StopKey, Limit > 0->
  case ets:lookup(Ref, Key ) of
    [Res]->
      [Res | iterate_stop_limit(ets:next(Ref,Key), Ref, StopKey, Limit-1 )];
    []->
      iterate_stop_limit(ets:next(Ref,Key), Ref, StopKey, Limit )
  end;
iterate_stop_limit(_Key, _Ref, _StopKey, _Limit )->
  [].

iterate_stop('$end_of_table', _Ref, _StopKey )->
  [];
iterate_stop(Key, Ref, StopKey ) when Key =< StopKey->
  case ets:lookup(Ref, Key ) of
    [Res]->
      [Res | iterate_stop(ets:next(Ref,Key), Ref, StopKey)];
    []->
      iterate_stop(ets:next(Ref,Key), Ref, StopKey )
  end;
iterate_stop(_Key, _Ref, _StopKey )->
  [].


iterate_ms_limit('$end_of_table', _Ref, _MS, _Limit )->
  [];
iterate_ms_limit(Key, Ref, MS, Limit ) when Limit > 0 ->
  case ets:match_spec_run(ets:lookup(Ref, Key ),MS) of
    [Res]->
      [Res | iterate_ms_limit(ets:next(Ref,Key), Ref, MS, Limit -1)];
    []->
      iterate_ms_limit(ets:next(Ref,Key), Ref, MS, Limit )
  end;
iterate_ms_limit(_Key, _Ref, _MS, _Limit )->
  [].

iterate_ms('$end_of_table', _Ref, _MS )->
  [];
iterate_ms(Key, Ref, MS )->
  case ets:match_spec_run(ets:lookup(Ref, Key ),MS) of
    [Res]->
      [Res | iterate_ms(ets:next(Ref,Key), Ref, MS)];
    []->
      iterate_ms(ets:next(Ref,Key), Ref, MS )
  end.

iterate('$end_of_table', _Ref )->
  [];
iterate(Key, Ref )->
  case ets:lookup(Ref, Key ) of
    [Res]->
      [Res | iterate(ets:next(Ref,Key), Ref)];
    []->
      iterate(ets:next(Ref,Key), Ref )
  end.

%----------------------FOLD LEFT------------------------------------------
foldl(#ref{ ets = Ref }, Query, UserFun, InAcc )->
  First =
    case Query of
      #{start := Start}-> Start;
      _->ets:first( Ref )
    end,
  Fun =
    case Query of
      #{ms:=MS}->
        CompiledMS = ets:match_spec_compile(MS),
        fun(Rec,Acc)->
          case ets:match_spec_run([Rec], CompiledMS) of
            [Res]->
              UserFun(Res,Acc);
            []->
              Acc
          end
        end;
      _->
        UserFun
    end,

  try
    case Query of
      #{ stop:=Stop }->
        do_foldl_stop( First, Ref, Fun, InAcc, Stop);
      _->
        do_foldl( First, Ref, Fun, InAcc )
    end
  catch
    {stop,Acc}->Acc
  end.

do_foldl_stop('$end_of_table', _Ref, _Fun, Acc, _StopKey)->
  Acc;
do_foldl_stop( Key, Ref, Fun, InAcc, StopKey ) when Key =< StopKey->
  case ets:lookup(Ref, Key) of
    [Rec]->
      Acc = Fun( Rec, InAcc ),
      do_foldl_stop( ets:next(Ref, Key), Ref, Fun, Acc, StopKey  );
    []->
      do_foldl_stop( ets:next(Ref, Key), Ref, Fun, InAcc, StopKey  )
  end;
do_foldl_stop(_Key, _Ref, _Fun, Acc, _StopKey)->
  Acc.

do_foldl( '$end_of_table', _Ref, _Fun, Acc )->
  Acc;
do_foldl( Key, Ref, Fun, InAcc )->
  case ets:lookup(Ref, Key) of
    [Rec]->
      Acc = Fun( Rec, InAcc ),
      do_foldl( ets:next(Ref, Key), Ref, Fun, Acc  );
    []->
      do_foldl( ets:next(Ref, Key), Ref, Fun, InAcc  )
  end.

%----------------------FOLD RIGHT------------------------------------------
foldr(#ref{ ets = Ref }, Query, UserFun, InAcc )->
  Last =
    case Query of
      #{start := Start}-> Start;
      _->ets:last( Ref )
    end,
  Fun =
    case Query of
      #{ms:=MS}->
        CompiledMS = ets:match_spec_compile(MS),
        fun(Rec,Acc)->
          case ets:match_spec_run([Rec], CompiledMS) of
            [Res]->
              UserFun(Res,Acc);
            []->
              Acc
          end
        end;
      _->
        UserFun
    end,

  try
    case Query of
      #{ stop:=Stop }->
        do_foldr_stop( Last, Ref, Fun, InAcc, Stop);
      _->
        do_foldr( Last, Ref, Fun, InAcc )
    end
  catch
    {stop,Acc}-> Acc
  end.

do_foldr_stop('$end_of_table', _Ref, _Fun, Acc, _StopKey )->
  Acc;
do_foldr_stop( Key, Ref, Fun, InAcc, StopKey ) when Key >= StopKey->
  case ets:lookup(Ref, Key) of
    [Rec]->
      Acc = Fun( Rec, InAcc ),
      do_foldr_stop( ets:prev(Ref, Key), Ref, Fun, Acc, StopKey  );
    []->
      do_foldr_stop( ets:prev(Ref, Key), Ref, Fun, InAcc, StopKey  )
  end;
do_foldr_stop(_Key, _Ref, _Fun, Acc, _StopKey)->
  Acc.

do_foldr('$end_of_table', _Ref, _Fun, Acc )->
  Acc;
do_foldr( Key, Ref, Fun, InAcc )->
  case ets:lookup(Ref, Key) of
    [Rec]->
      Acc = Fun( Rec, InAcc ),
      do_foldr( ets:prev(Ref, Key), Ref, Fun, Acc  );
    []->
      do_foldr( ets:prev(Ref, Key), Ref, Fun, InAcc  )
  end.

%%=================================================================
%%	INFO
%%=================================================================
get_size( Ref )->
  erlang:system_info(wordsize) * ets:info( Ref, memory ).



