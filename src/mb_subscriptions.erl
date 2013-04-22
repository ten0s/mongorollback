-module(mb_subscriptions).

-export([
	apply_rollbacks/3
]).

-type connection() :: pid().
-type collection() :: binary().
-type bson() :: bson:document().
-type reason() :: term().

%% ===================================================================
%% API
%% ===================================================================

-spec apply_rollbacks(connection(), collection(), [bson()]) -> ok | {error, reason()}.
apply_rollbacks(_ConnPid, _Coll, _Docs) ->
	ok.
