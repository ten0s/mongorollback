-module(mongorollback).

-export([
	main/1
]).

%% ===================================================================
%% API
%% ===================================================================

-spec main([string()]) -> no_return().
main([]) ->
	ScriptName = escript:script_name(),
	OptSpecs = opt_specs(),
	print_usage(ScriptName, OptSpecs);
main(Args) ->
	ok = application:start(mongodb),

	ScriptName = escript:script_name(),
	OptSpecs = opt_specs(),

	case getopt:parse(OptSpecs, Args) of
		{ok, {Opts, _NonOptArgs}} ->
			process_opts(ScriptName, Opts, OptSpecs);
		{error, {Reason, Data}} ->
			io:format(standard_error, "Error: ~s ~p~n~n", [Reason, Data]),
			halt(1)
	end.

%% ===================================================================
%% Internal
%% ===================================================================

opt_specs() ->
	[
		%% {Name, ShortOpt, LongOpt, ArgSpec, HelpMsg}
		{help, $?, "help", undefined, "Show this message"},
		%{debug, $d, "debug", {boolean, false}, "Enable debug output"},
		{host, $h, "host", {string, "localhost"}, "MongoDB server host name or IP address"},
		{port, $p, "port", {integer, 27017}, "Database server port"},
		{dir, undefined, undefined, string, "Rollback directory (mandatory)"}
    ].

process_opts(ScriptName, Opts, OptSpecs) ->
	case is_help(Opts) of
		true ->
			print_usage(ScriptName, OptSpecs);
		false ->
			case proplists:get_value(dir, Opts) of
				undefined ->
					io:format(standard_error, "Error: <dir> option is mandatory~n", []),
					halt(1);
				Dir ->
					Host = proplists:get_value(host, Opts),
					Port = proplists:get_value(port, Opts),
					apply_dir(Dir, Host, Port)
			end
	end.

is_help(Opts) ->
	case proplists:get_value(help, Opts) of
		undefined ->
			false;
		_ ->
			true
	end.

print_usage(ScriptName, OptSpecs) ->
	print_description_vsn(ScriptName),
	getopt:usage(OptSpecs, ScriptName).

print_description_vsn(ScriptName) ->
	case description_vsn(ScriptName) of
		{Description, Vsn} ->
			io:format("~s (~s)~n", [Description, Vsn]);
		_ ->
			ok
	end.

description_vsn(ScriptName) ->
	case script_options(ScriptName) of
		undefined ->
			undefined;
		Options ->
			Description = proplists:get_value(description, Options),
			Vsn = proplists:get_value(vsn, Options),
			{Description, Vsn}
	end.

script_options(ScriptName) ->
	{ok, Sections} = escript:extract(ScriptName, []),
	ZipArchive = proplists:get_value(archive, Sections),
	AppName = lists:flatten(io_lib:format("~p/ebin/~p.app", [?MODULE, ?MODULE])),
	case zip:extract(ZipArchive, [{file_list, [AppName]}, memory]) of
		{ok, [{AppName, Binary}]} ->
			{ok, Tokens, _} = erl_scan:string(binary_to_list(Binary)),
			{ok, {application, ?MODULE, Options}} = erl_parse:parse_term(Tokens),
			Options;
		_ ->
			undefined
	end.

apply_dir(Dir, Host, Port) ->
	case get_bson_filenames_from_dir(Dir) of
		{ok, Filenames} ->
			case build_rollbacks(Dir, Filenames, Host, Port) of
				{ok, Rollbacks} ->
					DbConns = proplists:get_keys(Rollbacks),
					case apply_rollbacks(DbConns, Rollbacks) of
						ok ->
							io:format("Done.~n");
						{error, Reason} ->
							io:format(standard_error, "Error: build_rollbacks(~p, ~p, ~p, ~p) ~p~n~n", [Dir, Filenames, Host, Port, Reason]),
							halt(1)
					end;
				{error, Reason} ->
					io:format(standard_error, "Error: build_rollbacks(~p, ~p, ~p, ~p) ~p~n~n", [Dir, Filenames, Host, Port, Reason]),
					halt(1)
			end;
		{error, Reason} ->
			io:format(standard_error, "Error: get_bson_filenames_from_dir(~p) ~p~n~n", [Dir, Reason]),
			halt(1)
	end.

get_bson_filenames_from_dir(Dir) ->
	case file:list_dir(Dir) of
		{ok, List} ->
			{ok, [Bson || Bson <- List, lists:suffix("bson", Bson)]};
		Error ->
			Error
	end.

build_rollbacks(Dir, Filenames, Host, Port) ->
	build_rollbacks(Dir, Filenames, Host, Port, []).

build_rollbacks(_, [], _, _, Acc) ->
	{ok, Acc};
build_rollbacks(Dir, [Filename|Filenames], Host, Port, Acc) ->
	case parse_bson_filename(Filename) of
		{ok, DbName, Coll} ->
			case get_bson_docs_from_file(Dir, Filename) of
				{ok, BsonDocs} ->
					NewAcc = [{{Host, Port, DbName}, {Coll, BsonDocs}} | Acc],
					build_rollbacks(Dir, Filenames, Host, Port, NewAcc);
				{error, Reason} ->
					io:format(standard_error, "Error: get_bson_from_file(~p, ~p) ~p~n~n", [Dir, Filename, Reason]),
					halt(1)
			end;
		{error, Reason} ->
			io:format(standard_error, "Error: parse_bson_filename ~p~n~n", [Reason]),
			halt(1)
	end.

parse_bson_filename(Filename) ->
	case string:tokens(Filename, ".") of
		[DbName, Coll, _, _, _] ->
			{ok, list_to_binary(DbName), list_to_binary(Coll)};
		_ ->
			{error, bad_name}
	end.

get_bson_docs_from_file(Dir, Filename) ->
	Fullname = filename:join(Dir, Filename),
	case file:read_file(Fullname) of
		{ok, Bin} ->
			get_bson_docs_from_bin(Bin);
	    Error ->
			Error
	end.

get_bson_docs_from_bin(Bin) ->
	get_bson_docs_from_bin(Bin, []).

get_bson_docs_from_bin(Bin, Acc) ->
	case bson_binary:get_document(Bin) of
		{BsonDoc, <<>>} ->
			{ok, [BsonDoc | Acc]};
		{BsonDoc, RestBin} ->
			get_bson_docs_from_bin(RestBin, [BsonDoc | Acc]);
		_ ->
			{error, bad_bson}
	end.

apply_rollbacks([], _) ->
	ok;
apply_rollbacks([DbConn|DbConns], Rollbacks) ->
	{Host, Port, DbName} = DbConn,
	Updates = proplists:append_values(DbConn, Rollbacks),
	case with_open_db(Host, Port, DbName,
		fun(ConnPid) -> apply_conn_rollbacks(ConnPid, Updates) end
	) of
		ok ->
			apply_rollbacks(DbConns, Rollbacks);
		Error ->
			Error
	end.

with_open_db(Host, Port, DbName, Fun) ->
	case connect(Host, Port, DbName) of
		{ok, ConnPid} ->
			Res = Fun(ConnPid),
			ok = disconnect(ConnPid),
			Res;
		Error ->
			Error
	end.

connect(Host, Port, DbName) ->
	Props = [
		{mongodb_conn_props, {single, {Host, Port}}},
		{mongodb_dbname, DbName},
		{mongodb_pool_size, 1}
	],
	mongodb_storage:start_link(Props).

disconnect(ConnPid) ->
	mongodb_storage:stop(ConnPid).

apply_conn_rollbacks(_, []) ->
	ok;
apply_conn_rollbacks(ConnPid, [{Coll, BsonDocs}|Rollbacks]) ->
	Module = list_to_atom(binary_to_list(Coll)),
	case Module:apply_rollbacks(ConnPid, Coll, BsonDocs) of
		ok ->
			apply_conn_rollbacks(ConnPid, Rollbacks);
		Error ->
			Error
	end.
