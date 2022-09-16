
-module(zaya_ets).

-ifndef(TEST).

-define(LOGERROR(Text),lager:error(Text)).
-define(LOGERROR(Text,Params),lager:error(Text,Params)).
-define(LOGWARNING(Text),lager:warning(Text)).
-define(LOGWARNING(Text,Params),lager:warning(Text,Params)).
-define(LOGINFO(Text),lager:info(Text)).
-define(LOGINFO(Text,Params),lager:info(Text,Params)).
-define(LOGDEBUG(Text),lager:debug(Text)).
-define(LOGDEBUG(Text,Params),lager:debug(Text,Params)).

-else.

-define(LOGERROR(Text),ct:pal("error: "++Text)).
-define(LOGERROR(Text,Params),ct:pal("error: "++Text,Params)).
-define(LOGWARNING(Text),ct:pal("warning: "++Text)).
-define(LOGWARNING(Text,Params),ct:pal("warning: "++Text,Params)).
-define(LOGINFO(Text),ct:pal("info: "++Text)).
-define(LOGINFO(Text,Params),ct:pal("info: "++Text,Params)).
-define(LOGDEBUG(Text),ct:pal("debug: "++Text)).
-define(LOGDEBUG(Text,Params),ct:pal("debug: "++Text,Params)).

-endif.

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

%%=================================================================
%%	COPY API
%%=================================================================
-export([

]).

%%=================================================================
%%	SERVICE
%%=================================================================
create( _Params )->
  ok.

open( _Params )->
  ets:new(?MODULE,[public,ordered_set]).

close( Ref )->
  catch ets:delete( Ref ),
  ok.

remove( _Params )->
  ok.

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(Ref, [Key|Rest])->
  case ets:lookup(Ref,Key) of
    [Rec]->
      [Rec | read(Ref, Rest)];
    _->
      read(Ref, Rest)
  end;
read(_Ref,[])->
  [].

write(Ref, KVs)->
  ets:insert( Ref, KVs ),
  ok.

delete(Ref,Keys)->
  [ ets:delete(Ref, K) || K <- Keys],
  ok.


%%=================================================================
%%	ITERATOR
%%=================================================================
first( Ref )->
  case ets:first( Ref ) of
    '$end_of_table'-> throw( undefined );
    Key->
      case ets:lookup(Ref, Key ) of
        [Rec]->
          Rec;
        _->
          next( Ref, Key )
      end
  end.

last( Ref )->
  case ets:last( Ref ) of
    '$end_of_table'-> throw( undefined );
    Key->
      case ets:lookup(Ref, Key ) of
        [Rec]->
          Rec;
        _->
          prev( Ref, Key )
      end
  end.

next( Ref, Key )->
  case ets:next( Ref, Key ) of
    '$end_of_table' -> throw( undefined );
    Next->
      case ets:lookup( Ref, Next ) of
        [Rec]-> Rec;
        _-> next( Ref, Next )
      end
  end.

prev( Ref, Key )->
  case ets:prev( Ref, Key ) of
    '$end_of_table' -> throw( undefined );
    Prev->
      case ets:lookup( Ref, Prev ) of
        [Rec]-> Rec;
        _-> prev( Ref, Prev )
      end
  end.

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
%----------------------FIND------------------------------------------
find(Ref, Query)->
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
          _->
            ets:tab2list( Ref )
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

%----------------------FOLD LEFT------------------------------------------
foldl( #ref{ref = Ref, read = Params}, Query, UserFun, InAcc )->
  StartKey =
    case Query of
      #{start := Start}-> ?ENCODE_KEY(Start);
      _->first
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

  {ok, Itr} = eleveldb:iterator(Ref, [{first_key, StartKey}|Params]),
  try
    case Query of
      #{ stop:=Stop }->
        do_foldl_stop( eleveldb:iterator_move(Itr, StartKey), Itr, Fun, InAcc, ?ENCODE_KEY(Stop) );
      _->
        do_foldl( eleveldb:iterator_move(Itr, StartKey), Itr, Fun, InAcc )
    end
  catch
    {stop,Acc}->Acc
  after
    catch eleveldb:iterator_close(Itr)
  end.

do_foldl_stop( {ok,K,V}, Itr, Fun, InAcc, StopKey ) when K =< StopKey->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldl_stop( eleveldb:iterator_move(Itr,prefetch), Itr, Fun, Acc, StopKey  );
do_foldl_stop(_, _Itr, _Fun, Acc, _StopKey )->
  Acc.

do_foldl( {ok,K,V}, Itr, Fun, InAcc )->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldl( eleveldb:iterator_move(Itr,prefetch), Itr, Fun, Acc  );
do_foldl(_, _Itr, _Fun, Acc )->
  Acc.

%----------------------FOLD RIGHT------------------------------------------
foldr( #ref{ref = Ref, read = Params}, Query, UserFun, InAcc )->
  StartKey =
    case Query of
      #{start := Start}-> ?ENCODE_KEY(Start);
      _->last
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

  {ok, Itr} = eleveldb:iterator(Ref, [{first_key, StartKey}|Params]),
  try
    case Query of
      #{ stop:=Stop }->
        do_foldr_stop( eleveldb:iterator_move(Itr, StartKey), Itr, Fun, InAcc, ?ENCODE_KEY(Stop) );
      _->
        do_foldr( eleveldb:iterator_move(Itr, StartKey), Itr, Fun, InAcc )
    end
  catch
    {stop,Acc}-> Acc
  after
    catch eleveldb:iterator_close(Itr)
  end.

do_foldr_stop( {ok,K,V}, Itr, Fun, InAcc, StopKey ) when K >= StopKey->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldr_stop( eleveldb:iterator_move(Itr,prev), Itr, Fun, Acc, StopKey  );
do_foldr_stop(_, _Itr, _Fun, Acc, _StopKey )->
  Acc.

do_foldr( {ok,K,V}, Itr, Fun, InAcc )->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldl( eleveldb:iterator_move(Itr,prev), Itr, Fun, Acc  );
do_foldr(_, _Itr, _Fun, Acc )->
  Acc.

%%=================================================================
%%	INFO
%%=================================================================
get_size( Ref )->
  get_size( Ref, 10 ).
get_size( #ref{dir = Dir} = R, Attempts ) when Attempts > 0->
  S = list_to_binary(os:cmd("du -s --block-size=1 "++ Dir)),
  case binary:split(S,<<"\t">>) of
    [Size|_]->
      try binary_to_integer( Size )
      catch _:_->
        % Sometimes du returns error when there are some file transformations
        timer:sleep(200),
        get_size( R, Attempts - 1 )
      end;
    _ ->
      timer:sleep(200),
      get_size( R, Attempts - 1 )
  end;
get_size( _R, 0 )->
  -1.

%%=================================================================
%%	COPY
%%=================================================================
%%fold(#source{ref = Ref}, Iterator, Acc0)->
%%  eleveldb:fold(Ref,Iterator, Acc0, [{first_key, first}]).
%%
%%write_batch(Batch, CopyRef)->
%%  eleveldb:write(CopyRef,Batch, [{sync, true}]).
%%
%%drop_batch(Batch0,#source{ref = Ref})->
%%  Batch =
%%    [case R of {put,K,_}->{delete,K};_-> R end || R <- Batch0],
%%  eleveldb:write(Ref,Batch, [{sync, false}]).
%%
%%action({K,V})->
%%  {{put,K,V},size(K)+size(V)}.
%%
%%live_action({write, {K,V}})->
%%  K1 = ?ENCODE_KEY(K),
%%  {K1, {put, K1,?ENCODE_VALUE(V)} };
%%live_action({delete,K})->
%%  K1 = ?ENCODE_KEY(K),
%%  {K1,{delete,K1}}.
%%
%%get_key({put,K,_V})->K;
%%get_key({delete,K})->K.
%%
%%decode_key(K)->?DECODE_KEY(K).
%%
%%rollback_copy( Target )->
%%  todo.


