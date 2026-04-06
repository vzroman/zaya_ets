-module(zaya_ets_writer_pool_worker).

-export([
  start_link/2,
  call/2
]).

-export([
  init/2
]).

-record(state, {
  table,
  batch_size,
  pending = []
}).

start_link(Table, #{batch_size := BatchSize})->
  proc_lib:start_link(?MODULE, init, [Table, BatchSize]).

call(Worker, Request)->
  %Monitor = erlang:monitor(process, Worker),
  Ref = test,
  Worker ! {pool_call, {self(), Ref}, Request},
  receive
    {pool_reply, Ref, Reply}->
      %erlang:demonitor(Monitor, [flush]),
      handle_reply(Reply)
    % {'DOWN', Monitor, process, Worker, Reason}->
    %   exit(Reason)
  end.

init(Table, BatchSize)->
  proc_lib:init_ack({ok, self()}),
  loop(#state{
    table = Table,
    batch_size = BatchSize
  }).

loop(State0)->
  {Request, State1} = next_request(State0),
  {Batch, State2} = collect_requests([Request], request_size(Request), State1),
  Reply = flush_batch(Batch, State2),
  reply_batch(Batch, Reply),
  loop(State2).

next_request(#state{pending = [Request | Rest]} = State)->
  {Request, State#state{pending = Rest}};
next_request(State)->
  receive
    {pool_call, From, Request}->
      {{From, Request}, State}
  end.

collect_requests(Batch, Count, #state{batch_size = BatchSize} = State) when Count < BatchSize->
  case next_immediate(State) of
    {ok, Request, State1}->
      collect_requests([Request | Batch], Count + request_size(Request), State1);
    empty->
      {lists:reverse(Batch), State}
  end;
collect_requests(Batch, _Count, State)->
  {lists:reverse(Batch), State}.

next_immediate(#state{pending = [Request | Rest]} = State)->
  {ok, Request, State#state{pending = Rest}};
next_immediate(State)->
  receive
    {pool_call, From, Request}->
      {ok, {From, Request}, State}
  after
    0 ->
      empty
  end.

request_size({_From, {ops, Ops}})->
  length(Ops).

flush_batch(Batch, #state{table = Table, batch_size = BatchSize})->
  Ops = lists:append([RequestOps || {_From, {ops, RequestOps}} <- Batch]),
  try
    apply_ops(Table, Ops, BatchSize),
    ok
  catch
    Class:Reason:Stack->
      {raise, Class, Reason, Stack}
  end.

apply_ops(_Table, [], _BatchSize)->
  ok;
apply_ops(Table, Ops, BatchSize)->
  {Chunk, Rest} = take_ops(Ops, BatchSize, []),
  apply_chunk(Table, Chunk),
  apply_ops(Table, Rest, BatchSize).

take_ops(Rest, 0, Acc)->
  {lists:reverse(Acc), Rest};
take_ops([], _Count, Acc)->
  {lists:reverse(Acc), []};
take_ops([Op | Rest], Count, Acc)->
  take_ops(Rest, Count - 1, [Op | Acc]).

apply_chunk(_Table, [])->
  ok;
apply_chunk(Table, Ops)->
  lists:foreach(
    fun
      ({put, Key, Value})->
        true = ets:insert(Table, {Key, Value}),
        ok;
      ({delete, Key})->
        true = ets:delete(Table, Key),
        ok
    end,
    Ops
  ).

reply_batch(Batch, Reply)->
  [reply(From, Reply) || {From, _Request} <- Batch],
  ok.

reply({Pid, Monitor}, Reply)->
  Pid ! {pool_reply, Monitor, Reply},
  ok.

handle_reply({raise, Class, Reason, Stack})->
  erlang:raise(Class, Reason, Stack);
handle_reply(Reply)->
  Reply.
