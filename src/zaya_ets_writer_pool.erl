-module(zaya_ets_writer_pool).

-behaviour(supervisor).

-export([
  start_link/2,
  call/2,
  stop/1,
  register_worker/2
]).

-export([
  init/1
]).

-define(REF(Ref),{?MODULE,Ref}).
-define(MAX_RESTARTS, 10).
-define(MAX_PERIOD, 1000).
-define(STOP_TIMEOUT, 5000).

start_link(Table, Opts)->
  Owner = self(),
  supervisor:start_link(?MODULE, [Table, Opts, Owner]).

call(Pool, Request)->
  Worker = worker(Pool),
  zaya_ets_writer_pool_worker:call(Worker, Request).

stop(Supervisor) when is_pid(Supervisor)->
  case is_process_alive(Supervisor) of
    true->
      unlink(Supervisor),
      Monitor = erlang:monitor(process, Supervisor),
      exit(Supervisor, shutdown),
      receive
        {'DOWN', Monitor, process, Supervisor, _Reason}->
          ok
      end;
    false->
      ok
  end.

init([Table, #{size := Size} = Opts, Owner]) ->
  Ref = ?REF(self()),
  Workers =
    [#{
      id => N,
      start => {zaya_ets_writer_pool_worker, start_link, [Table, Opts, N, Ref]},
      restart => permanent,
      shutdown => ?STOP_TIMEOUT,
      type => worker,
      modules => [zaya_ets_writer_pool_worker]
    } || N <- lists:seq(1, Size)],

  persistent_term:put(
    Ref,
    #{
      workers => list_to_tuple([undefined || _<-Workers]),
      counter => atomics:new(1, [{signed, false}]),
      size => Size
    }
  ),

  Supervisor = #{
    strategy => one_for_one,
    intensity => ?MAX_RESTARTS,
    period => ?MAX_PERIOD
  },

  init_guard( Owner ),

  {ok, {Supervisor, Workers}}.

worker(Ref)->
  #{
    workers := Workers,
    counter := Counter,
    size := Size
  } = persistent_term:get(?REF(Ref)),

  I = atomics:add_get(Counter, 1, 1),
  Index = I rem Size,
  if
    I =:= Size ->
      atomics:sub(Counter, 1, Size);
    true ->
      ok
  end,
  element(Index+1, Workers).

init_guard( Owner )->
  Self = self(),
  spawn(
    fun()->
      erlang:monitor(process, Owner),
      erlang:monitor(process, Self),
      guard_loop(Owner, Self)
    end
  ),
  ok.

guard_loop(Owner, Supervisor)->
  receive
    {'DOWN', _Ref, process, Owner, Reason}->
      exit(Supervisor, Reason);
    {'DOWN', _Ref, process, Supervisor, Reason}->
      ok;
    _Unexpected->
      guard_loop(Owner, Supervisor)
  end.

