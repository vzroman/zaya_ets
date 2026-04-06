-module(zaya_ets_pool_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
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
  write_is_synchronous_test/1,
  delete_is_synchronous_test/1,
  commit_and_empty_requests_test/1,
  commit1_commit2_equivalent_test/1,
  concurrent_write_callers_test/1,
  batch_order_and_boundaries_test/1,
  dump_batch_and_reopen_test/1
]).

all()->
  [
    write_is_synchronous_test,
    delete_is_synchronous_test,
    commit_and_empty_requests_test,
    commit1_commit2_equivalent_test,
    concurrent_write_callers_test,
    batch_order_and_boundaries_test,
    dump_batch_and_reopen_test
  ].

groups()->
  [].

init_per_suite(Config)->
  Config.

end_per_suite(_Config)->
  ok.

init_per_group(_Group, Config)->
  Config.

end_per_group(_Group, _Config)->
  ok.

init_per_testcase(_TestCase, Config)->
  Config.

end_per_testcase(_TestCase, _Config)->
  ok.

write_is_synchronous_test(_Config)->
  with_ref(
    fun(Ref)->
      ok = zaya_ets:write(Ref, [{alpha, 1}]),
      ?assertEqual([{alpha, 1}], zaya_ets:read(Ref, [alpha]))
    end
  ).

delete_is_synchronous_test(_Config)->
  with_ref(
    fun(Ref)->
      ok = zaya_ets:write(Ref, [{alpha, 1}]),
      ok = zaya_ets:delete(Ref, [alpha]),
      ?assertEqual([], zaya_ets:read(Ref, [alpha]))
    end
  ).

commit_and_empty_requests_test(_Config)->
  with_ref(
    fun(Ref)->
      ok = zaya_ets:write(Ref, [{keep, 1}, {drop, 2}]),
      ok = zaya_ets:commit(Ref, [{keep, 3}, {add, 4}], [drop]),
      ?assertEqual(#{keep => 3, add => 4}, read_map(Ref, [keep, add])),
      ?assertEqual([], zaya_ets:read(Ref, [drop])),

      ok = zaya_ets:write(Ref, []),
      ok = zaya_ets:delete(Ref, []),
      ok = zaya_ets:commit(Ref, [], []),

      ?assertEqual(#{keep => 3, add => 4}, read_map(Ref, [keep, add])),
      ?assertEqual([], zaya_ets:read(Ref, [drop]))
    end
  ).

commit1_commit2_equivalent_test(_Config)->
  with_ref(
    fun(Ref)->
      ok = zaya_ets:write(Ref, [{keep, 1}, {drop, 2}]),
      Token = zaya_ets:commit1(Ref, [{keep, 10}, {add, 11}], [drop]),
      ok = zaya_ets:commit2(Ref, Token),
      ?assertEqual(#{keep => 10, add => 11}, read_map(Ref, [keep, add])),
      ?assertEqual([], zaya_ets:read(Ref, [drop]))
    end
  ).

concurrent_write_callers_test(_Config)->
  with_ref(
    fun(Ref)->
      Parent = self(),
      Keys = lists:seq(1, 8),
      [
        spawn(fun()->
          Parent ! {writer_result, Key, catch zaya_ets:write(Ref, [{Key, Key * 10}])}
        end)
       || Key <- Keys
      ],
      Results = collect_results(length(Keys), []),
      ?assertEqual([], [Result || {_Key, Result} <- Results, Result =/= ok]),
      ?assertEqual(
        maps:from_list([{Key, Key * 10} || Key <- Keys]),
        read_map(Ref, Keys)
      )
    end
  ).

batch_order_and_boundaries_test(_Config)->
  with_ref(
    fun(Ref)->
      ok = zaya_ets:commit(
        Ref,
        [{victim, 1}, {keep_a, 2}, {keep_b, 3}, {keep_c, 4}],
        [victim]
      ),
      ?assertEqual([], zaya_ets:read(Ref, [victim])),
      ?assertEqual(
        #{keep_a => 2, keep_b => 3, keep_c => 4},
        read_map(Ref, [keep_a, keep_b, keep_c])
      )
    end
  ).

dump_batch_and_reopen_test(_Config)->
  Ref1 = new_ref(),
  try
    ok = zaya_ets:write(Ref1, [{queued, 1}]),
    ?assertEqual([{queued, 1}], zaya_ets:read(Ref1, [queued]))
  after
    ok = zaya_ets:close(Ref1)
  end,

  Ref2 = new_ref(),
  try
    ok = zaya_ets:dump_batch(Ref2, [{raw_a, 10}, {raw_b, 20}]),
    ?assertEqual(
      #{raw_a => 10, raw_b => 20},
      read_map(Ref2, [raw_a, raw_b])
    ),
    ok = zaya_ets:write(Ref2, [{after_reopen, 99}]),
    ?assertEqual([{after_reopen, 99}], zaya_ets:read(Ref2, [after_reopen]))
  after
    ok = zaya_ets:close(Ref2)
  end.

with_ref(Fun)->
  Ref = new_ref(),
  try
    Fun(Ref)
  after
    ok = zaya_ets:close(Ref)
  end.

new_ref()->
  zaya_ets:create(#{
    writer_pool => #{
      size => 1,
      batch_size => 4
    }
  }).

read_map(Ref, Keys)->
  maps:from_list(zaya_ets:read(Ref, Keys)).

collect_results(0, Results)->
  Results;
collect_results(Count, Results)->
  receive
    {writer_result, Key, Result}->
      collect_results(Count - 1, [{Key, Result} | Results])
  after
    5000 ->
      ct:fail(timeout)
  end.
