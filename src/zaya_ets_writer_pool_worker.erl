-module(zaya_ets_writer_pool_worker).

-export([
  start_link/4,
  call/2
]).

-export([
  init/2
]).

start_link(Table, #{batch_size := BatchSize}, N, Ref)->
  proc_lib:start_link(?MODULE, init, [Table, BatchSize]).

call(Worker, Requests)->
  Monitor = erlang:monitor(process, Worker),
  Worker ! {pool_call, {self(), Monitor}, Requests},
  receive
    {pool_reply, Monitor, Reply}->
      erlang:demonitor(Monitor, [flush]),
      handle_reply(Reply);
    {'DOWN', Monitor, process, Worker, Reason}->
      exit(Reason)
  end.

-record(state,{
  table,
  max_size
}).
-record(batch,{
  type,
  data,
  size,
  reply
}).

init(Table, BatchSize)->
  proc_lib:init_ack({ok, self()}),
  loop(
    _Batch = undefined,
    #state{
      table = Table,
      max_size = BatchSize
    }
  ).

loop(
  Batch0,
  State = #state{
    max_size = MaxSize,
    table = Table
  }
)->
  {Batches, NextBatch} = collect_requests(MaxSize, Batch0),
  flush_batches(Batches, Table),
  loop(NextBatch, State).

collect_requests(
    MaxSize,
    _Batch = undefined
)->
  receive
    {pool_call, From, Ops}->
      case merge_requests(From, Ops, _Batch = undefined) of
        [Batch] ->
          collect_requests(MaxSize, Batch);
        Batches0->
          {Batches, [Next]} = lists:split(length(Batches0)-1, Batches0),
          {Batches, Next}
      end
  end;
collect_requests(
  MaxSize,
  Batch0 = #batch{
    size = BatchSize
  }
) when BatchSize < MaxSize->
  receive
    {pool_call, From, Ops}->
      case merge_requests(From, Ops, Batch0) of
        [Batch] ->
          collect_requests(MaxSize, Batch);
        Batches0->
          {Batches, [Next]} = lists:split(length(Batches0)-1, Batches0),
          {Batches, Next}
      end
  after
    0 -> {[Batch0], _NextBatch = undefined}
  end;
collect_requests(
  _MaxSize,
  Batch = #batch{}
)->
  {[Batch], _NextBatch = undefined}.


merge_requests(
    From,
    [{Type, Data}|Rest],
    _Batch = undefined
)->
  Batch = #batch{
    type = Type,
    data = [Data],
    size = length(Data),
    reply = []
  },
  merge_requests(From, Rest, Batch);
merge_requests(
  From,
  [{Type,Data} | Rest],
  Batch0 = #batch{
    type = BatchType,
    size = BatchSize,
    data = BatchData
  }
)->
  if
    Type =:= BatchType ->
      Batch = Batch0#batch{
        size = BatchSize + length(Data),
        data = [Data|BatchData]
      },
      merge_requests(From, Rest, Batch);
    true ->
      NextBatch = #batch{
        type = Type,
        data = [Data],
        size = length(Data),
        reply = []
      },
      [Batch0| merge_requests(From, Rest, NextBatch)]
  end;
merge_requests(
  From,
  [],
  Batch0 = #batch{
    reply = ReplyTo
  }
)->
  Batch = Batch0#batch{
    reply = [From|ReplyTo]
  },
  [Batch].

flush_batches(
  [#batch{
    type = Type,
    data = DataList,
    reply = ReplyTo
  } | Rest],
  Table
)->
  Reply =
    try
      Data = lists:append(lists:reverse( DataList )),
      if
        Type =:= write -> ets:insert(Table, Data);
        Type =:= delete -> [ ets:delete(Table, K)|| K <- Data ]
      end,
      ok
    catch
      Class:Reason:Stack->
        {raise, Class, Reason, Stack}
    end,
  [ reply( From, Reply ) || From <- lists:reverse(ReplyTo) ],

  flush_batches(Rest, Table);
flush_batches([], _Table)->
  ok.

reply({Pid, Monitor}, Reply)->
  Pid ! {pool_reply, Monitor, Reply},
  ok.

handle_reply({raise, Class, Reason, Stack})->
  erlang:raise(Class, Reason, Stack);
handle_reply(Reply)->
  Reply.
