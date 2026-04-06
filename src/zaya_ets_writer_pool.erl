-module(zaya_ets_writer_pool).

-behaviour(supervisor).

-export([
  start_link/2,
  call/2,
  stop/1
]).

-export([
  init/1
]).

-define(MAX_RESTARTS, 10).
-define(MAX_PERIOD, 1000).
-define(STOP_TIMEOUT, 5000).

start_link(Table, #{size := Size, batch_size := BatchSize} = Opts)->
  {ok, Supervisor} = supervisor:start_link(?MODULE, [Table, Opts]),
  true = unlink(Supervisor),
  {ok, #{
    supervisor => Supervisor,
    workers => workers_tuple(Supervisor, Size),
    counter => atomics:new(1, [{signed, false}]),
    size => Size,
    batch_size => BatchSize
  }}.

call(Pool, Request)->
  Worker = worker(Pool),
  zaya_ets_writer_pool_worker:call(Worker, Request).

stop(#{supervisor := Supervisor}) when is_pid(Supervisor)->
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

init([Table, #{size := Size} = Opts]) ->
  Workers =
    [#{
      id => N,
      start => {zaya_ets_writer_pool_worker, start_link, [Table, Opts]},
      restart => permanent,
      shutdown => ?STOP_TIMEOUT,
      type => worker,
      modules => [zaya_ets_writer_pool_worker]
    } || N <- lists:seq(0, Size - 1)],

  Supervisor = #{
    strategy => one_for_one,
    intensity => ?MAX_RESTARTS,
    period => ?MAX_PERIOD
  },

  {ok, {Supervisor, Workers}}.

worker(#{counter := Counter, size := Size} = Pool)->
  I = atomics:add_get(Counter, 1, 1),
  Index = I rem Size,
  if
    I =:= Size ->
      atomics:sub(Counter, 1, Size);
    true ->
      ok
  end,
  worker_pid(Pool, Index).

worker_pid(#{workers := Workers} = Pool, Index)->
  Worker = element(Index + 1, Workers),
  case is_process_alive(Worker) of
    true ->
      Worker;
    false ->
      refreshed_worker_pid(Pool, Index)
  end.

refreshed_worker_pid(#{supervisor := Supervisor}, Index)->
  case lists:keyfind(Index, 1, supervisor:which_children(Supervisor)) of
    {Index, Worker, worker, _Modules} when is_pid(Worker) ->
      Worker;
    _ ->
      exit({pool_worker_not_found, Index})
  end.

workers_tuple(Supervisor, Size)->
  Workers =
    lists:sort(
      [{Id, Worker} || {Id, Worker, worker, _Modules} <- supervisor:which_children(Supervisor)]
    ),
  true = (length(Workers) =:= Size),
  list_to_tuple([Worker || {_Id, Worker} <- Workers]).
