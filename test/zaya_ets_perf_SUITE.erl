-module(zaya_ets_perf_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
  suite/0,
  all/0,
  groups/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_group/2,
  end_per_group/2,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  pure_concurrent_writes_benchmark/1
]).

-define(SMOKE_PROCESSES, 10).
-define(SMOKE_WRITES_PER_PROCESS, 100).
-define(DEFAULT_PROCESSES, 1000).
-define(DEFAULT_WRITES_PER_PROCESS, 1000).
-define(DEFAULT_ENTRIES_PER_WRITE, 100).
-define(DEFAULT_READY_TIMEOUT_MS, 3600000).
-define(DEFAULT_COMPLETION_TIMEOUT_MS, 3600000).
-define(DEFAULT_GROUP, massive_pure_concurrent_writes).

suite()->
  [{timetrap, {hours, 2}}].

all()->
  [{group, ?DEFAULT_GROUP}].

groups()->
  [
    {
      smoke_pure_concurrent_writes,
      [parallel],
      [pure_concurrent_writes_benchmark || _ <- lists:seq(1, ?SMOKE_PROCESSES)]
    },
    {
      massive_pure_concurrent_writes,
      [parallel],
      [pure_concurrent_writes_benchmark || _ <- lists:seq(1, ?DEFAULT_PROCESSES)]
    }
  ].

init_per_suite(Config)->
  Config.

end_per_suite(_Config)->
  ok.

init_per_group(Group, Config) when Group =:= smoke_pure_concurrent_writes; Group =:= massive_pure_concurrent_writes ->
  BenchmarkConfig = benchmark_config(Group),
  {ok, Coordinator, Ref} =
    start_group_coordinator(
      Group,
      ?config(priv_dir, Config),
      BenchmarkConfig
    ),
  [
    {benchmark_group, Group},
    {benchmark_config, BenchmarkConfig},
    {benchmark_ref, Ref},
    {benchmark_coordinator, Coordinator},
    {worker_counter, atomics:new(1, [{signed, false}])}
    | Config
  ];
init_per_group(_Group, Config)->
  Config.

end_per_group(Group, Config) when Group =:= smoke_pure_concurrent_writes; Group =:= massive_pure_concurrent_writes ->
  ok = stop_group_coordinator(?config(benchmark_coordinator, Config));
end_per_group(_Group, _Config)->
  ok.

init_per_testcase(pure_concurrent_writes_benchmark, Config)->
  BenchmarkConfig = ?config(benchmark_config, Config),
  WorkerIndex = atomics:add_get(?config(worker_counter, Config), 1, 1),
  Batches =
    prepare_batches(
      WorkerIndex,
      maps:get(writes_per_process, BenchmarkConfig),
      maps:get(entries_per_write, BenchmarkConfig)
    ),
  [
    {worker_index, WorkerIndex},
    {worker_batches, Batches}
    | Config
  ];
init_per_testcase(_TestCase, Config)->
  Config.

end_per_testcase(_TestCase, _Config)->
  ok.

%% The benchmark worker payload is fully prepared in init_per_testcase/2.
%% The testcase itself only waits on the shared start barrier, performs writes,
%% and reports completion.
pure_concurrent_writes_benchmark(Config)->
  Coordinator = ?config(benchmark_coordinator, Config),
  Ref = ?config(benchmark_ref, Config),
  WorkerIndex = ?config(worker_index, Config),
  Batches = ?config(worker_batches, Config),
  BenchmarkConfig = ?config(benchmark_config, Config),
  Monitor = erlang:monitor(process, Coordinator),

  try
    Coordinator ! {worker_ready, self(), WorkerIndex},
    ok =
      await_coordinator(
        Coordinator,
        Monitor,
        WorkerIndex,
        benchmark_start,
        maps:get(ready_timeout_ms, BenchmarkConfig),
        benchmark_start_timeout
      ),

    ok = pure_write_loop(Ref, Batches),

    Coordinator ! {worker_done, self(), WorkerIndex},
    await_coordinator(
      Coordinator,
      Monitor,
      WorkerIndex,
      benchmark_done,
      maps:get(completion_timeout_ms, BenchmarkConfig),
      benchmark_done_timeout
    )
  after
    erlang:demonitor(Monitor, [flush])
  end.

benchmark_config(smoke_pure_concurrent_writes)->
  #{
    processes => ?SMOKE_PROCESSES,
    writes_per_process => ?SMOKE_WRITES_PER_PROCESS,
    entries_per_write => ?DEFAULT_ENTRIES_PER_WRITE,
    ready_timeout_ms => ?DEFAULT_READY_TIMEOUT_MS,
    completion_timeout_ms => ?DEFAULT_COMPLETION_TIMEOUT_MS,
    backend_params => #{}
  };
benchmark_config(massive_pure_concurrent_writes)->
  #{
    processes => ?DEFAULT_PROCESSES,
    writes_per_process => ?DEFAULT_WRITES_PER_PROCESS,
    entries_per_write => ?DEFAULT_ENTRIES_PER_WRITE,
    ready_timeout_ms => ?DEFAULT_READY_TIMEOUT_MS,
    completion_timeout_ms => ?DEFAULT_COMPLETION_TIMEOUT_MS,
    backend_params => #{}
  }.

start_group_coordinator(Group, PrivDir, BenchmarkConfig)->
  Parent = self(),
  {Coordinator, Monitor} =
    erlang:spawn_monitor(fun()->
      coordinator_init(Parent, Group, PrivDir, BenchmarkConfig)
    end),
  receive
    {coordinator_ready, Coordinator, Ref}->
      erlang:demonitor(Monitor, [flush]),
      {ok, Coordinator, Ref};
    {coordinator_failed, Coordinator, Reason, Stack}->
      erlang:demonitor(Monitor, [flush]),
      ct:fail({coordinator_failed, Reason, Stack})
    ;
    {'DOWN', Monitor, process, Coordinator, Reason}->
      ct:fail({coordinator_start_failed, Reason})
  after
    10000 ->
      erlang:demonitor(Monitor, [flush]),
      ct:fail({coordinator_start_timeout, Group})
  end.

stop_group_coordinator(Coordinator)->
  Monitor = erlang:monitor(process, Coordinator),
  Coordinator ! {stop, self()},
  receive
    {stopped, Coordinator}->
      erlang:demonitor(Monitor, [flush]),
      ok;
    {'DOWN', Monitor, process, Coordinator, Reason}->
      ct:fail({coordinator_stopped_unexpectedly, Reason})
  after
    10000 ->
      erlang:demonitor(Monitor, [flush]),
      ct:fail(coordinator_stop_timeout)
  end.

coordinator_init(Parent, Group, PrivDir, BenchmarkConfig)->
  try
    Ref = zaya_ets:create(maps:get(backend_params, BenchmarkConfig)),
    Parent ! {coordinator_ready, self(), Ref},
    coordinator_loop(
      #{
        group => Group,
        priv_dir => PrivDir,
        benchmark_config => BenchmarkConfig,
        ref => Ref,
        ready_workers => [],
        ready_count => 0,
        done_count => 0,
        start_native => undefined
      }
    )
  catch
    Class:Reason:Stack->
      Parent ! {coordinator_failed, self(), {Class, Reason}, Stack}
  end.

coordinator_loop(
  #{
    benchmark_config := #{processes := WorkerCount} = BenchmarkConfig,
    ready_workers := ReadyWorkers,
    ready_count := ReadyCount
  } = State
) ->
  receive
    {worker_ready, WorkerPid, _WorkerIndex} ->
      ReadyWorkers1 = [WorkerPid | ReadyWorkers],
      ReadyCount1 = ReadyCount + 1,
      if
        ReadyCount1 =:= WorkerCount ->
          StartNative = erlang:monotonic_time(),
          [Pid ! benchmark_start || Pid <- ReadyWorkers1],
          coordinator_loop(
            State#{
              ready_workers => [],
              ready_count => ReadyCount1,
              start_native => StartNative
            }
          );
        true ->
          coordinator_loop(
            State#{
              ready_workers => ReadyWorkers1,
              ready_count => ReadyCount1
            }
          )
      end;
    {worker_done, WorkerPid, _WorkerIndex} ->
      DoneCount1 = maps:get(done_count, State) + 1,
      if
        DoneCount1 =:= WorkerCount ->
          Result =
            result(
              BenchmarkConfig,
              erlang:monotonic_time() - maps:get(start_native, State)
            ),
          write_result(Result, maps:get(priv_dir, State), maps:get(group, State)),
          print_result(Result),
          WorkerPid ! benchmark_done,
          coordinator_loop(State#{done_count => DoneCount1});
        true ->
          WorkerPid ! benchmark_done,
          coordinator_loop(State#{done_count => DoneCount1})
      end;
    {stop, ReplyTo} ->
      ok = zaya_ets:close(maps:get(ref, State)),
      ReplyTo ! {stopped, self()},
      ok
  end.

await_coordinator(Coordinator, Monitor, WorkerIndex, Message, Timeout, TimeoutTag)->
  receive
    Message ->
      ok;
    {'DOWN', Monitor, process, Coordinator, Reason} ->
      ct:fail({coordinator_down, WorkerIndex, Message, Reason})
  after
    Timeout ->
      ct:fail({TimeoutTag, WorkerIndex})
  end.

prepare_batches(ProcessIndex, WritesPerProcess, EntriesPerWrite)->
  ProcessBase = (ProcessIndex - 1) * WritesPerProcess * EntriesPerWrite,
  prepare_batches(ProcessBase, WritesPerProcess, EntriesPerWrite, 0, []).

prepare_batches(_ProcessBase, WritesPerProcess, _EntriesPerWrite, WritesPerProcess, Acc)->
  lists:reverse(Acc);
prepare_batches(ProcessBase, WritesPerProcess, EntriesPerWrite, WriteIndex, Acc)->
  BatchBase = ProcessBase + (WriteIndex * EntriesPerWrite),
  Batch = prepare_batch(BatchBase, EntriesPerWrite, 1, []),
  prepare_batches(
    ProcessBase,
    WritesPerProcess,
    EntriesPerWrite,
    WriteIndex + 1,
    [Batch | Acc]
  ).

prepare_batch(_BatchBase, EntriesPerWrite, EntryIndex, Acc) when EntryIndex > EntriesPerWrite->
  lists:reverse(Acc);
prepare_batch(BatchBase, EntriesPerWrite, EntryIndex, Acc)->
  Key = BatchBase + EntryIndex,
  prepare_batch(BatchBase, EntriesPerWrite, EntryIndex + 1, [{Key, Key} | Acc]).

pure_write_loop(_Ref, [])->
  ok;
pure_write_loop(Ref, [Batch | Rest])->
  ok = zaya_ets:write(Ref, Batch),
  pure_write_loop(Ref, Rest).

result(Config, ElapsedNative)->
  Processes = maps:get(processes, Config),
  WritesPerProcess = maps:get(writes_per_process, Config),
  EntriesPerWrite = maps:get(entries_per_write, Config),
  TotalWrites = Processes * WritesPerProcess,
  TotalEntries = TotalWrites * EntriesPerWrite,
  ElapsedUs = erlang:convert_time_unit(ElapsedNative, native, microsecond),
  ElapsedSeconds = ElapsedUs / 1000000,
  #{
    processes => Processes,
    writes_per_process => WritesPerProcess,
    entries_per_write => EntriesPerWrite,
    total_writes => TotalWrites,
    total_entries => TotalEntries,
    elapsed_us => ElapsedUs,
    writes_per_second => rate(TotalWrites, ElapsedSeconds),
    entries_per_second => rate(TotalEntries, ElapsedSeconds)
  }.

rate(_Count, +0.0)->
  infinity;
rate(Count, Seconds)->
  Count / Seconds.

write_result(Result, PrivDir, Group)->
  ResultPath =
    filename:join(
      PrivDir,
      atom_to_list(Group) ++ "_zaya_ets_perf_result.term"
    ),
  ok = file:write_file(ResultPath, io_lib:format("~p.~n", [Result])),
  ct:pal("performance result written to ~s", [ResultPath]),
  ok.

print_result(Result)->
  ct:pal(
    "zaya_ets perf result~n"
    "  processes: ~p~n"
    "  writes/process: ~p~n"
    "  entries/write: ~p~n"
    "  total writes: ~p~n"
    "  total entries: ~p~n"
    "  elapsed us: ~p~n"
    "  writes/s: ~.2f~n"
    "  entries/s: ~.2f",
    [
      maps:get(processes, Result),
      maps:get(writes_per_process, Result),
      maps:get(entries_per_write, Result),
      maps:get(total_writes, Result),
      maps:get(total_entries, Result),
      maps:get(elapsed_us, Result),
      maps:get(writes_per_second, Result),
      maps:get(entries_per_second, Result)
    ]
  ).
