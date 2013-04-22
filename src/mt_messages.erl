-module(mt_messages).

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
apply_rollbacks(_, _, []) ->
	ok;
apply_rollbacks(ConnPid, Coll, [Doc|Docs]) ->
	case utils:safe_foreach(
		fun({Field, Method}) ->
			case bson:at(Field, Doc) of
				null -> ok;
				___  -> Method(ConnPid, Coll, Doc)
			end
		end,
		[{'rqt', fun apply_req/3}, {'rpt', fun apply_resp/3}, {'dt', fun apply_dlr/3}],
		ok, {error, '_'}
	) of
		ok ->
			apply_rollbacks(ConnPid, Coll, Docs);
		Error ->
			Error
	end.

%% ===================================================================
%% Internal
%% ===================================================================

apply_req(ConnPid, Coll, Doc) ->
	ReqId = bsondoc:at('ri', Doc),
	InMsgId = bsondoc:at('imi', Doc),
	CustomerId = bsondoc:at('ci', Doc),
	UserId = bsondoc:at('ui', Doc),
	GatewayId = bsondoc:at('gi', Doc),
	ClientType = bsondoc:at('ct', Doc),
	Body = bsondoc:at('b', Doc),
	Encoding = bsondoc:at('e', Doc),
	Type = bsondoc:at('t', Doc),
	DstAddr = bsondoc:at('da', Doc),
	SrcAddr = bsondoc:at('sa', Doc),
	RegDlr = bsondoc:at('rd', Doc),
	EsmClass = bsondoc:at('ec', Doc),
	ValPeriod = bsondoc:at('vp', Doc),
	ReqTime = bsondoc:at('rqt', Doc),

	Selector = {
		'ri' , ReqId,
		'imi', InMsgId
	},
	Modifier = {
		'$set', {
			'ci' , CustomerId,
			'ui' , UserId,
			'ct' , ClientType,
			'gi' , GatewayId,
			'b'  , Body,
			'e'  , Encoding,
			't'  , Type,
			'da' , DstAddr,
			'sa' , SrcAddr,
			'rd' , RegDlr,
			'ec' , EsmClass,
			'vp' , ValPeriod,
			'rqt', ReqTime
		}
	},
	io:format("~p~n~p~n~p~n~p~n~n", [ConnPid, Coll, Selector, Modifier]),
	mongodb_storage:upsert(ConnPid, Coll, Selector, Modifier).

apply_resp(ConnPid, Coll, Doc) ->
	ReqId = bsondoc:at('ri', Doc),
	InMsgId = bsondoc:at('imi', Doc),
	OutMsgId = bsondoc:at('omi', Doc),
	RespStatus = bsondoc:at('rps', Doc),
	RespTime = bsondoc:at('rpt', Doc),

	Selector = {
		'ri' , ReqId,
		'imi', InMsgId
	},
	Modifier = {
		'$set', {
			'omi', OutMsgId,
			'rps', RespStatus,
			'rpt', RespTime
		}
	},
	io:format("~p~n~p~n~p~n~p~n~n", [ConnPid, Coll, Selector, Modifier]),
	mongodb_storage:upsert(ConnPid, Coll, Selector, Modifier).

apply_dlr(ConnPid, Coll, Doc) ->
	GatewayId = bsondoc:at('gi', Doc),
	OutMsgId = bsondoc:at('omi', Doc),
	DlrStatus = bsondoc:at('ds', Doc),
	DlrTime = bsondoc:at('dt', Doc),

	Selector = {
		'gi' , GatewayId,
		'omi', OutMsgId
	},
	Modifier = {
		'$set', {
			'dt', DlrTime,
			'ds', bsondoc:atom_to_binary(DlrStatus)
		}
	},
	io:format("~p~n~p~n~p~n~p~n~n", [ConnPid, Coll, Selector, Modifier]),
	mongodb_storage:upsert(ConnPid, Coll, Selector, Modifier).
