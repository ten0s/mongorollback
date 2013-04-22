.PHONY: doc

REBAR=./rebar

all: get-deps compile escriptize

get-deps:
	@$(REBAR) get-deps

update-deps:
	@$(REBAR) update-deps

escriptize: compile
	@$(REBAR) escriptize

compile: get-deps
	@$(REBAR) compile

clean:
	@$(REBAR) clean
