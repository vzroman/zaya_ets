-module(zaya_ets_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([
    all/0,
    groups/0,
    init_per_testcase/2,
    end_per_testcase/2,
    init_per_group/2,
    end_per_group/2,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    low_level_api_tests/1,
    first_tests/1,
    last_tests/1,
    next_tests/1,
    prev_tests/1,
    find_tests/1,
    foldl_tests/1,
    foldr_tests/1
]).

-define(GET(Key, Config), proplists:get_value(Key, Config)).
-define(GET(Key, Config, Default), proplists:get_value(Key, Config, Default).

all() ->
    [
        low_level_api_tests,
        first_tests,
        last_tests,
        next_tests,
        prev_tests,
        find_tests,
        foldl_tests,
        foldr_tests
    ].

groups() ->
    [].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    TabId = zaya_ets:open(params),
    [{table, TabId} | Config].

end_per_testcase(_, Config) ->
    Storage = ?GET(table, Config),
    zaya_ets:close(Storage).

low_level_api_tests(Config) ->
    TabId = ?GET(table, Config),

    % fill the storage with records
    Count = 20000,
    [
        begin
            Record = {N, ok},
            ok = zaya_ets:write(TabId, Record)
        end
     || N <- lists:seq(1, Count)
    ],

    [{1, ok}] = zaya_ets:read(TabId, [1]),
    [{20, ok}] = zaya_ets:read(TabId, [20]),
    [{10020, ok}] = zaya_ets:read(TabId, [10020]),
    [{20000, ok}] = zaya_ets:read(TabId, [20000]),
    [{1, ok}, {20, ok}, {10020, ok}, {20000, ok}] = zaya_ets:read(TabId, [1, 20, 10020, 20000]),

    ok = zaya_ets:delete(TabId, [1]),
    ok = zaya_ets:delete(TabId, [20]),
    ok = zaya_ets:delete(TabId, [10020]),
    ok = zaya_ets:delete(TabId, [20000]),
    ok = zaya_ets:delete(TabId, [10, 30, 10030, 10090]),

    [] = zaya_ets:read(TabId, []),
    [] = zaya_ets:read(TabId, [1]),
    [] = zaya_ets:read(TabId, [20]),
    [] = zaya_ets:read(TabId, [10020]),
    [] = zaya_ets:read(TabId, [20000]),
    [] = zaya_ets:read(TabId, [1, 10, 20, 30, 10020, 10030, 10090, 20000]),

    [{2, ok}] = zaya_ets:read(TabId, [2]),
    [{22, ok}] = zaya_ets:read(TabId, [22]),
    [{10022, ok}] = zaya_ets:read(TabId, [10022]),
    [{2, ok}, {22, ok}, {10022, ok}] = zaya_ets:read(TabId, [2, 22, 10022]).

first_tests(Config) ->
    TabId = ?GET(table, Config),

    % fill the storage with records
    Count = 20000,
    [
        begin
            Record = {N, ok},
            ok = zaya_ets:write(TabId, Record)
        end
     || N <- lists:seq(1, Count)
    ],

    {1, ok} = zaya_ets:first(TabId),

    % Delete every second key in the storage
    [
        begin
            ok = zaya_ets:delete(TabId, [N])
        end
     || N <- lists:seq(1, Count, 2)
    ],

    {2, ok} = zaya_ets:first(TabId),

    % Remove all records from the storage
    [
        begin
            ok = zaya_ets:delete(TabId, [N])
        end
     || N <- lists:seq(1, Count)
    ],

    undefined = (catch zaya_ets:first(TabId)).

last_tests(Config) ->
    TabId = ?GET(table, Config),

    % fill the storage with records
    Count = 20000,
    [
        begin
            Record = {N, ok},
            ok = zaya_ets:write(TabId, Record)
        end
     || N <- lists:seq(1, Count)
    ],

    {Count, ok} = zaya_ets:last(TabId),

    % Delete last key in the storage
    ok = zaya_ets:delete(TabId, [Count]),
    {19999, ok} = zaya_ets:last(TabId),

    % Remove all records from the storage
    [
        begin
            ok = zaya_ets:delete(TabId, [N])
        end
     || N <- lists:seq(1, Count)
    ],

    undefined = (catch zaya_ets:last(TabId)).

next_tests(Config) ->
    TabId = ?GET(table, Config),

    % fill the storage with records
    Count = 20000,
    [
        begin
            Record = {N, ok},
            ok = zaya_ets:write(TabId, Record)
        end
     || N <- lists:seq(1, Count)
    ],

    {1, ok} = zaya_ets:next(TabId, 0),
    {2, ok} = zaya_ets:next(TabId, 1),
    {21, ok} = zaya_ets:next(TabId, 20),
    {10001, ok} = zaya_ets:next(TabId, 10000),
    {10021, ok} = zaya_ets:next(TabId, 10020),
    undefined = (catch zaya_ets:next(TabId, 20000)),

    % Delete every second key in the storage
    [
        begin
            ok = zaya_ets:delete(TabId, [N])
        end
     || N <- lists:seq(1, Count, 2)
    ],

    {2, ok} = zaya_ets:next(TabId, 0),
    {2, ok} = zaya_ets:next(TabId, 1),
    {22, ok} = zaya_ets:next(TabId, 20),
    {10002, ok} = zaya_ets:next(TabId, 10000),
    {10022, ok} = zaya_ets:next(TabId, 10020),
    undefined = (catch zaya_ets:next(TabId, 20000)).

prev_tests(Config) ->
    TabId = ?GET(table, Config),

    % fill the storage with records
    Count = 20000,
    [
        begin
            Record = {N, ok},
            ok = zaya_ets:write(TabId, Record)
        end
     || N <- lists:seq(1, Count)
    ],

    undefined = (catch zaya_ets:prev(TabId, 1)),
    {1, ok} = zaya_ets:prev(TabId, 2),
    {20, ok} = zaya_ets:prev(TabId, 21),
    {10000, ok} = zaya_ets:prev(TabId, 10001),
    {10020, ok} = zaya_ets:prev(TabId, 10021),
    {20000, ok} = zaya_ets:prev(TabId, 20001),

    % Delete every second key in the storage
    [
        begin
            ok = zaya_ets:delete(TabId, [N])
        end
     || N <- lists:seq(1, Count, 2)
    ],

    undefined = (catch zaya_ets:prev(TabId, 1)),
    undefined = (catch zaya_ets:prev(TabId, 2)),
    {2, ok} = zaya_ets:prev(TabId, 4),
    {20, ok} = zaya_ets:prev(TabId, 22),
    {10000, ok} = zaya_ets:prev(TabId, 10001),
    {10000, ok} = zaya_ets:prev(TabId, 10002),
    {10020, ok} = zaya_ets:prev(TabId, 10022),
    {20000, ok} = zaya_ets:prev(TabId, 20001).

find_tests(Config) ->
    TabId = ?GET(table, Config),

    % fill the storage with records
    Count = 20000,
    [
        begin
            Record = {N, N rem 10, dog},
            ok = zaya_ets:write(TabId, Record)
        end
     || N <- lists:seq(1, Count div 2)
    ],
    [
        begin
            Record = {N, N rem 10, cat},
            ok = zaya_ets:write(TabId, Record)
        end
     || N <- lists:seq((Count div 2) + 1, Count)
    ],

    %---Ful Query---
    MatchSpec = [
        {
            {'$1', '$2', '$3'},
            [
                {'andalso', {'>=', '$2', 0}, {'<', '$2', 5}, {'=:=', '$3', dog}}
            ],
            ['$_']
        }
    ],

    MatchSpec1 = [
        {
            {'$1', '$2', '$3'},
            [
                {'andalso', {'>=', '$2', 5}, {'=<', '$2', 9}, {'=:=', '$3', cat}}
            ],
            ['$_']
        }
    ],

    MatchSpec2 = [
        {
            {'$1', '$2', '$3'},
            [
                {'orelse', {'<', '$1', 2}, {'>', '$1', 10000}}
            ],
            ['$_']
        }
    ],

    Query = #{
        start => 9995,
        stop => 10010,
        ms => MatchSpec,
        limit => 10
    },

    Query1 = #{
        start => 9995,
        stop => 10010,
        ms => MatchSpec1,
        limit => 3
    },

    Query2 = #{
        stop => 10010,
        ms => MatchSpec2,
        limit => 2
    },

    [{10000, 0, dog}] = zaya_ets:find(TabId, Query),
    [{10005, 5, cat}, {10006, 6, cat}, {10007, 7, cat}] = zaya_ets:find(TabId, Query1),
    [{1, 1, dog}, {10001, 1, cat}] = zaya_ets:find(TabId, Query2),

    %---ms_stop---
    MatchSpec3 = [
        {
            {'$1', '$2', '$3'},
            [
                {'=:=', '$2', 3}
            ],
            ['$_']
        }
    ],

    MS_stop = #{
        stop => 20,
        ms => MatchSpec3
    },

    MS_stop1 = #{
        start => 10001,
        stop => 10020,
        ms => MatchSpec3
    },

    [{3, 3, dog}, {13, 3, dog}] = zaya_ets:find(TabId, MS_stop),
    [{10003, 3, cat}, {10013, 3, cat}] = zaya_ets:find(TabId, MS_stop1),

    %---stop_limit---
    Stop_limit = #{
        stop => 20,
        limit => 3
    },

    Stop_limit1 = #{
        stop => 3,
        limit => 20
    },

    [{1, 1, dog}, {2, 2, dog}, {3, 3, dog}] = zaya_ets:find(TabId, Stop_limit),
    [{1, 1, dog}, {2, 2, dog}, {3, 3, dog}] = zaya_ets:find(TabId, Stop_limit1),

    %---stop---
    Stop_only = #{
        start => 10500,
        stop => 10502
    },

    [{10500, 0, cat}, {10501, 1, cat}, {10502, 2, cat}] = zaya_ets:find(TabId, Stop_only),

    %---ms_limit---
    MS_limit = #{
        ms => MatchSpec3,
        limit => 3
    },

    MS_limit1 = #{
        start => 10001,
        ms => MatchSpec3,
        limit => 3
    },

    [{3, 3, dog}, {13, 3, dog}, {23, 3, dog}] = zaya_ets:find(TabId, MS_limit),
    [{10003, 3, cat}, {10013, 3, cat}, {10023, 3, cat}] = zaya_ets:find(TabId, MS_limit1),

    %---ms---
    MatchSpec4 = [
        {
            {'$1', '$2', '$3'},
            [
                {'orelse', 
                    {'andalso', {'<', '$1', 20}, {'=:=', '$2', 4}},
                    {'andalso', {'>', '$1', 10000}, {'<', '$1', 10020}, {'=:=', '$2', 2}}
                }
            ],
            ['$_']
        }
    ],

    MS_only = #{
        ms => MatchSpec4
    },

    MS_only1 = #{
        start => 10001,
        ms => MatchSpec4
    },

    [{4, 4, dog}, {14, 4, dog}, {10002, 2, cat}, {10012, 2, cat}] = zaya_ets:find(TabId, MS_only),
    [{10002, 2, cat}, {10012, 2, cat}] = zaya_ets:find(TabId, MS_only1),

    %---Start_only---
    Start_only = #{
        start => 19997
    },

    [{19997, 7, cat}, {19998, 8, cat}, {19999, 9, cat}, {20000, 0, cat}] = zaya_ets:find(
        TabId, Start_only
    ),

    %---empty table---
    [
        begin
            ok = zaya_ets:delete(TabId, [N])
        end
     || N <- lists:seq(1, Count)
    ],

    %only works for #{ms=>MS, limit=>Limit} Query
    undefined = (catch zaya_ets:find(TabId, MS_limit)),

    %---tab2list case---
    [
        begin
            Record = {N, dog},
            ok = zaya_ets:write(TabId, Record)
        end
     || N <- lists:seq(1, 3)
    ],

    [{1, dog}, {2, dog}, {3, dog}] = zaya_ets:find(TabId, #{}).

foldl_tests(Config) ->
    TabId = ?GET(table, Config),

    % fill the storage with records
    Count = 20000,
    [
        begin
            Record = {N, N rem 100},
            ok = zaya_ets:write(TabId, Record)
        end
     || N <- lists:seq(1, Count)
    ],

    MatchSpec = [
        {
            {'$1', '$2'},
            [
                {'=:=', {'rem', '$2', 2}, 0}
            ],
            ['$_']
        }
    ],

    MS_stop = #{
        ms => MatchSpec,
        stop => 10000
    },

    MS_only = #{
        ms => MatchSpec
    },

    Empty_query = #{},

    MaxFun =
        fun
            ({Key, Value}, B) when (Key + Value) > B -> Key + Value;
            (_, B) -> B
        end,

    {FirstKey, FirstValue} = zaya_ets:first(TabId),
    Start = FirstKey + FirstValue,

    10096 = zaya_ets:foldl(TabId, MS_stop, MaxFun, Start),
    20096 = zaya_ets:foldl(TabId, MS_only, MaxFun, Start),
    20098 = zaya_ets:foldl(TabId, Empty_query, MaxFun, Start).

foldr_tests(Config) ->
    TabId = ?GET(table, Config),

    % fill the storage with records
    Count = 20000,
    [
        begin
            Record = {N, N rem 100},
            ok = zaya_ets:write(TabId, Record)
        end
     || N <- lists:seq(1, Count)
    ],

    MatchSpec = [
        {
            {'$1', '$2'},
            [
                {'=:=', {'rem', '$2', 2}, 0}
            ],
            ['$_']
        }
    ],

    MS_stop = #{
        ms => MatchSpec,
        stop => 10000
    },

    MS_only = #{
        ms => MatchSpec
    },

    Empty_query = #{},

    MinFun =
        fun
            ({Key, Value}, B) when (Key + Value) < B -> Key + Value;
            (_, B) -> B
        end,

    {FirstKey, FirstValue} = zaya_ets:last(TabId),
    Start = FirstKey + FirstValue,

    10000 = zaya_ets:foldr(TabId, MS_stop, MinFun, Start),
    4 = zaya_ets:foldr(TabId, MS_only, MinFun, Start),
    2 = zaya_ets:foldr(TabId, Empty_query, MinFun, Start).