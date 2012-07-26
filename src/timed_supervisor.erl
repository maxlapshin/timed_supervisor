%%%------------------------------------------------------------------------
%%% File: $Id$
%%%------------------------------------------------------------------------
%%% @doc Timed supervisor allows to run an application on
%%% given days of week and hours of day.  This supervisor is modeled after
%%% `supervisor_bridge' with additions of timer processing and day analysis
%%% functions.
%%% ```
%%%   Examples:
%%%     Run {M,F,A} every Monday between 9:30am and 5:00pm:
%%%       timed_supervisor:start_link({M,F,A}, [{schedule, [{mon, [{"9:30:00", "17:00:00"}]}]}]).
%%%
%%%     Run {M,F,A} every Mon,Tue,Wed between 9:30am and 5:00pm:
%%%       timed_supervisor:start_link({M,F,A}, [{schedule, [{[mon,tue,wed], [{{9,30,0}, {17,0,0}}]}]}]).
%%%
%%%     Run {M,F,A} every Mon between 9am and 5pm, and Wed between 8am and 9pm:
%%%       timed_supervisor:start_link({M,F,A}, [{schedule, [{mon, [{{9,0,0}, {17,0,0}}]},
%%%                                                         {wed, [{{8,0,0}, {21,0,0}}]}]}]).
%%%
%%%     Run {M,F,A} every Monday between 9:00am and 5:00pm, and set the maximum
%%%     restart intensity to 3 failures within 20 seconds with delay between
%%%     restarts of 4 seconds:
%%%       timed_supervisor:start_link(
%%%             {M,F,A}, [{schedule, [{mon, [{"9:00:00", "17:00:00"}]}]},
%%%                       {restart, {3, 20, 4}}]).
%%% '''
%%% @end
%%%------------------------------------------------------------------------
%%% @type day()         = mon | tue | wed | thu | fri | sat | sun
%%% @type time()        = string() | {Hour::integer(), Min::integer(), Sec::integer()}.
%%%                       Time can be represented as a "HH:MM:SS" string, 
%%%                       "HH:MM" string or a tuple.
%%% @type hourrange()   = {FromTime::time(), ToTime::time()}
%%% @type schedule()    = [ DaySchedule ]
%%%         DaySchedule = {DayRange, Hours}
%%%         DayRange    = any | day() | [ day() ]
%%%         Hours       = [ hourrange() ]
%%% @type timespec()    = {Mon::hourrange(), Tue::hourrange(), Wed::hourrange(),
%%%                        Thu::hourrange(), Fri::hourrange(), Sat::hourrange(),
%%%                        Sun::hourrange()}
%%% @type mfa()         = {M::atom(), F::atom(), Args::list()}
%%% @type sup_name()    = atom() | pid()
%%% @type sup_options() = [SupOption]
%%%         SupOption   = {schedule, Schedule::schedule()} |
%%%                       {restart, RestartSpec} |
%%%                       {onfailure, FailFun} |
%%%                       use_monitor | {use_monitor, boolean()} |
%%%                       {report_type, ReportType::term()} |
%%%                       {shutdown, ShutdownType}
%%%         RestartSpec = {MaxR::integer(), MaxTime::integer(), Delay::integer()}}
%%%         FailFun     = (SupName::sup_name(), ChildMFA::mfa(), Reason,
%%%                         {Restarts::integer(), MaxR::integer()}) -> FailAction |
%%%                       FailAction
%%%         FailAction  = reschedule | shutdown | {shutdown, Reason} | default.
%%%     Timed Supervisor's options.
%%%     <dl>
%%%     <dt>Schedule</dt>
%%%         <dd>Contains a schedule when supervised {M,F,A} needs to run.</dd>
%%%     <dt>RestartSpec</dt>
%%%         <dd>If specified, the failed {M,F,A} will be restarted up to `MaxR' times
%%%             in `MaxTime' seconds with `Delay' number of seconds between
%%%             successive restarts. By default RestartSpec is
%%%             `{_MaxR = 0, _MaxT = 1, _Delay = 0}' (auto restart is disabled)</dd>
%%%     <dt>FailFun</dt>
%%%         <dd>If specified as a function with arity 4, it will be called on
%%%             failure of {M,F,A} when its failure reason is other than
%%%             'normal' or 'shutdown'. The return value of the function
%%%             can alter restart behavior of supervised {M,F,A}.</dd>
%%%     <dt>use_monitor</dt>
%%%         <dd>When specified the superviser will use monitor instead of
%%%             a link to the child process. The implication is that it'll
%%%             be able to detect the death of the child, but when the
%%%             supervisor process is killed with `kill' reason the child
%%%             process will remain running.</dd>
%%%     <dt>ReportType</dt>
%%%         <dd>When specified, the `error_logger:error_report({Type, ReportType}, Report)'
%%%             and `error_logger:info_report({Type, ReportType}, Report)'
%%%             will be used instead of `error_logger:*_report/1' calls
%%%             (with Type = supervisor_report | progress), so that a custom
%%%             event logger can be installed to log these events.</dd>
%%%     <dt>ShutdownType</dt>
%%%         <dd>`brutal_kill' - kill child using `exit(Pid, kill)'.</dd>
%%%         <dd>`integer() >= 0' - kill child with `exit(Pid, shutdown)' followed
%%%             by `exit(Pid, kill)' after this number of milleseconds.</dd>
%%%     <dt>FailAction</dt>
%%%         <dd>`reschedule' - schedule ChildMFA for the next time interval
%%%                            in the schedule().</dd>
%%%         <dd>`shutdown'   - exit immediately with `shutdown' reason without any
%%%                            further restart attempts.</dd>
%%%         <dd>`{exit, Reason}' - exit immediately with given `Reason'
%%%                            without any further restart attempts.</dd>
%%%         <dd>`default'    - proceed with default restart action.</dd>
%%%     </dl>
%%%
%%% @author  Serge Aleynikov <saleyn@gmail.com>
%%% @version $Revision$
%%%          $Date$
%%% @end
%%%------------------------------------------------------------------------
%%% Created 2006-10-18 Serge Aleynikov <saleyn@gmail.com>
%%% $URL$
%%%------------------------------------------------------------------------
%%% ``The contents of this file are subject to the Erlang Public License,
%%% Version 1.1, (the "License"); you may not use this file except in
%%% compliance with the License. You should have received a copy of the
%%% Erlang Public License along with this software. If not, it can be
%%% retrieved via the world wide web at http://www.erlang.org/.
%%%
%%% Software distributed under the License is distributed on an "AS IS"
%%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%% the License for the specific language governing rights and limitations
%%% under the License.
%%%
%%% The Initial Developer of the Original Code is Serge Aleynikov.
%%% Portions created by Ericsson are Copyright 1999, Ericsson Utvecklings
%%% AB. All Rights Reserved.''
%%%------------------------------------------------------------------------
-module(timed_supervisor).

-behaviour(gen_server).

%% External exports
-export([
    start/3, start/2, start_link/3, start_link/2,
    swap_child/3, terminate_child/1, reschedule_child/1, restart_child/1,
    full_schedule/0, weekdays/0, dow_to_string/1, info/1, which_children/1
]).

%% Internal exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-export([validate_schedule/1, print_timespec/1]).

%% Test cases
-export([test/0, test_mfa/3, get_next_time/2, get_following_time/2]).

-record(state, {
    mfa,                % {M,F,A} to execute
    pid,                % Child pid
    status=scheduled,   % scheduled | failed | running | restarting | terminated
    name,               % name of the child
    schedule,           % Normalized scheduling mask
    next_run=now,       % now | datetime(). DateTime of the next run.
    until_time,         % the job will run until this time.
    reported=false,     % True when progress report was printed at least once for next interval.
    last_start,         % Time of last start
    last_fail,          % Time of last failure
    intensity = 0,      % Max number of restarts allowed within period
    period    = 1,      % Time in seconds in which the number of 'restarts' is allowed
    restarts  = 0,      % Current number of restarts in 'period'
    delay,              % Delay time in seconds between successive restarts
    use_monitor=false,  % When true - use monitor instead of linking to child
    monref,             % When use_monitor is true, this value is the monitor reference
    shutdown=2000,      % brutal_kill | integer() >= 0
    report_type,        % When different from `undefined', error_logger:error_report/2
                        % is used for error reports.
    start_timer,        % Timer reference used to start the child process
    stop_timer,         % Timer reference used to stop the child process
    onfailure=default   % fun/4 | reschedule | shutdown | {exit, Reason} | default -
                        % controls failure action
}).

-define(COMPILED_SPEC, '$timespec$').

%%-------------------------------------------------------------------------
%% @spec (Name::sup_name(), ChildMFA::mfa(), SupOpts::sup_options()) ->
%%              {ok, Pid::pid()} | ignore | {error, Error}
%%
%% @doc Creates a supervisor process as part of a supervision tree.  This
%%      function will, among other things, ensure that the supervisor is
%%      linked to the calling process (its supervisor).  The child process
%%      started by `{M, F, A}' will only be run between time windows
%%      specified by the `schedule' option.  `RestartSpec' option allows
%%      to define MaxR number of restarts within MaxTime window
%%      (in seconds) with a restart Delay seconds.  When maximum restart
%%      intensity is reached and the process keeps failing, this supervisor
%%      process fails with reason `reached_max_restart_intensity'. Note that
%%      default `restart' option is `{_MaxR = 0, _MaxT = 1, _Delay = 0}'
%%      meaning that the child process won't be auto restarted.
%%
%% @see //stdlib/supervisor
%% @end
%%-------------------------------------------------------------------------
start_link(Name, ChildMFA = {_M, _F, _A}, SupOpts) when is_list(SupOpts) ->
    gen_server:start_link({local, Name}, ?MODULE, [ChildMFA, SupOpts, Name], []).

%%-------------------------------------------------------------------------
%% @spec (ChildMFA::mfa(), SupOpts::sup_options()) ->
%%              {ok, Pid::pid()} | ignore | {error, Error}
%% @doc Creates a timed supervisor as part of supervision tree without
%%      a registered name.
%% @see start_link/3
%% @end
%%-------------------------------------------------------------------------
start_link(ChildMFA = {_M, _F, _A}, SupOpts) when is_list(SupOpts) ->
    gen_server:start_link(?MODULE, [ChildMFA, SupOpts, self], []).

%%-------------------------------------------------------------------------
%% @equiv start_link/3
%% @doc Creates a timed supervisor process outside of supervision tree.
%% @see start_link/3
%% @end
%%-------------------------------------------------------------------------
start(Name, ChildMFA = {_M, _F, _A}, SupOpts) when is_list(SupOpts) ->
    gen_server:start({local, Name}, ?MODULE, [ChildMFA, SupOpts, Name], []).

%%-------------------------------------------------------------------------
%% @equiv start_link/2
%% @doc Creates a timed supervisor process outside of supervision tree.
%% @see start_link/3
%% @end
%%-------------------------------------------------------------------------
start(ChildMFA = {_M, _F, _A}, SupOpts) when is_list(SupOpts) ->
    gen_server:start(?MODULE, [ChildMFA, SupOpts, self], []).

%%-------------------------------------------------------------------------
%% @spec (Name) -> State::list()
%% @doc Return supervisor's internal state.
%% @end
%%-------------------------------------------------------------------------
info(Name) ->
    gen_server:call(Name, info).

%%-------------------------------------------------------------------------
%% @spec (Name, MFA::mfa(), SupOptions::sup_options()) -> ok | {error, Why}
%% @doc Replace child's specification and restart (according to schedule)
%%      the child process if it was running.
%% @end
%%-------------------------------------------------------------------------
swap_child(Name, {_M, _F, _A} = NewMFA, SupOptions) ->
    gen_server:call(Name, {swap_child, NewMFA, SupOptions}).

%%-------------------------------------------------------------------------
%% @spec (Name) -> ok | {error, Why}
%% @doc Terminate child and supervisor with reason `normal'.
%% @end
%%-------------------------------------------------------------------------
terminate_child(Name) ->
    gen_server:call(Name, terminate_child).

%%-------------------------------------------------------------------------
%% @spec (Name, Reason) -> ok | {error, Why}
%% @doc Terminate child with reason `normal' and reschedule it for
%%      the next interval in the schedule specification.
%% @end
%%-------------------------------------------------------------------------
reschedule_child(Name) ->
    gen_server:call(Name, reschedule_child).

%%-------------------------------------------------------------------------
%% @spec (Name, Reason) -> ok | {error, Why}
%% @doc Bounce child process with reason `normal'.  If process is not 
%%      scheduled to run, nothing happens and the supervisor will activate
%%      it at the future scheduled time.
%% @end
%%-------------------------------------------------------------------------
restart_child(Name) ->
    gen_server:call(Name, restart_child).

%%-------------------------------------------------------------------------
%% @spec (Name) -> [] | [Pid::pid()]
%% @doc Return a list possibly containing a running child process.
%% @end
%%-------------------------------------------------------------------------
which_children(Name) ->
    gen_server:call(Name, which_children).

%%-------------------------------------------------------------------------
%% @spec () -> schedule()
%% @doc Return a schedule covering all days of week / hours of day.  This
%%      is a convenience function for a default schedule value.
%% @end
%%-------------------------------------------------------------------------
full_schedule() ->
    [{[mon,tue,wed,thu,fri,sat,sun], [{{0,0,0},{23,59,59}}]}].

%%-------------------------------------------------------------------------
%% @spec () -> [Day::atom()]
%% @doc Return a list of weekday atoms
%% @end
%%-------------------------------------------------------------------------
weekdays() ->
    [mon,tue,wed,thu,fri].

-define(FMT(Fmt,Args), lists:flatten(io_lib:format(Fmt,Args))).

%%-------------------------------------------------------------------------
%% Callback functions from gen_server
%%-------------------------------------------------------------------------

%% @private
init([MFA={M, _F, _A}, SupOpts, Name0]) ->
    process_flag(trap_exit, true),
    try
        State0 = validate_sup_opts(SupOpts, #state{schedule=full_schedule()}),
        {?COMPILED_SPEC, SchedSpec} = validate_schedule(State0#state.schedule),
        Name   = supname(Name0, M),
        case start_child(State0#state{name=Name, mfa=MFA, schedule=SchedSpec}, start) of
        {ok, _Status, State} -> {ok, State};
        {error, Error}       -> {stop, Error}
        end
    catch _:{error, Reason} ->
        {stop, Reason};
    _:Reason ->
        {stop, Reason}
end.

%% A supervisor *must* answer the supervisor:which_children call.
%% @private
handle_call(which_children, _From, State) ->
    case State#state.pid of
    Pid when is_pid(Pid) -> {reply, [Pid], State};
    undefined            -> {reply, [], State}
    end;

handle_call({swap_child, MFA, SupOptions}, _From, State0) ->
    try
        State1 = validate_sup_opts(SupOptions, State0),
        {?COMPILED_SPEC, SchedSpec} = validate_schedule(State1#state.schedule),
        case start_child(State1#state{name=State0#state.name, mfa=MFA, schedule=SchedSpec}, restart) of
        {ok, _Status, State} ->
            {reply, ok, State};
        {error, Reason} ->
            {stop, shutdown, {error, Reason}, State0}
        end
    catch _:{error, Why} ->
        {reply, {error, Why}, State0};
    _:Why ->
        {reply, {error, Why}, State0}
    end;

handle_call(terminate_child, _From, State) ->
    stop_child(State, terminated, shutdown),
    {stop, shutdown, ok, State#state{monref=undefined}};

handle_call(reschedule_child, _From, State) ->
    case start_child(State, reschedule) of
    {ok, _Status, NewState} -> {reply, ok, NewState};
    {error, Reason}         -> {stop, shutdown, {error, Reason}, State}
    end;

handle_call(restart_child, _From, State) ->
    stop_child(State, restart, normal),
    case start_child(State, restart) of
    {ok, _Status, NewState} -> {reply, ok, NewState};
    {error, Reason}         -> {stop, shutdown, {error, Reason}, State}
    end;

handle_call(info, _From, State) ->
    Reply = [
        {pid,           State#state.pid},
        {status,        State#state.status},
        {mfa,           State#state.mfa},
        {next_run,      State#state.next_run},
        {last_start,    now_to_datetime(State#state.last_start)},
        {last_fail,     now_to_datetime(State#state.last_fail)},
        {restart,       {State#state.intensity, State#state.period, State#state.delay}},
        {restarts,      State#state.restarts},
        {use_monitor,   State#state.use_monitor},
        {report_type,   State#state.report_type},
        {shutdown,      State#state.shutdown}
    ],
    {reply, Reply, State};
handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

now_to_datetime(undefined) ->
    undefined;
now_to_datetime(Now) ->
    calendar:now_to_universal_time(Now).

%% @private
handle_cast(_, State) ->
    {noreply, State}.

%% @private
handle_info({timer, Type}, State) when Type=:=restart; Type=:=scheduled_stop; Type=:=reschedule ->
    case start_child(State, Type) of
    {ok, _, NewState} ->
        {noreply, NewState};
    {error, Reason} ->
        {stop, Reason, State}
    end;

handle_info({'DOWN', Ref, process, Pid, Reason}, #state{use_monitor=true, monref=Ref} = State) ->
    handle_info({'EXIT', Pid, Reason}, State);
handle_info({'EXIT', Pid, Reason}, #state{pid=Pid} = State) ->
    #state{intensity=I, period=P, last_fail=T, restarts=C, delay=D} = State,
    case FailAction = do_onfailure(Reason, State) of
    default ->
        Now = now(),
        if Reason =:= normal; Reason =:= shutdown ->
            do_report_shutdown([{onfailure, FailAction}], Reason, State#state{status=terminated});
        true ->
            case in_period(T, Now, P) of
            true when C < I ->
                % We haven't reached restart intensity yet
                NewState = State#state{restarts=C+1, status=restarting},
                Context = [child_terminated, {count, C+1, I}, {delay_s, D}],
                report_error(Context, Reason, NewState),
                erlang:send_after(D*1000, self(), {timer, restart}),
                {noreply, NewState#state{pid=undefined}};
            true ->
                % Restart intensity is reached - bail out
                do_report_shutdown([{onfailure, FailAction}], reached_max_restart_intensity, State#state{status=failed});
            false when I =:= 0 ->
                do_report_shutdown([no_restarts_requested], Reason, State#state{status=failed});
            false ->
                % This infrequent failure is outside of the period window.
                NewState = State#state{restarts=1, last_fail=Now, status=restarting},
                Context = [child_terminated, {count, 1, I}, {delay_s, D}],
                report_error(Context, Reason, NewState),
                erlang:send_after(D*1000, self(), {timer, restart}),
                {noreply, NewState#state{pid=undefined}}
            end
        end;
    _ when FailAction =:= reschedule; FailAction =:= restart ->
        NewState = State#state{restarts=1, status=scheduled},
        Context  = [child_terminated, {onfailure, FailAction}, {restart, 1, I}],
        report_error(Context, Reason, NewState),
        handle_info({timer, FailAction}, NewState#state{pid=undefined});
    shutdown ->
        do_report_shutdown([], shutdown, State#state{restarts=C+1, status=terminated});
    {exit, Why} ->
        do_report_shutdown([{onfailure, {exit, Why}}], Why, State#state{restarts=C+1, status=terminated})
    end;
handle_info(_, State) ->
    {noreply, State}.

do_report_shutdown(FailAction, Reason, State) ->
    Context = [child_terminated] ++ FailAction ++ [{restart, State#state.restarts, State#state.intensity}],
    report_error(Context, Reason, State),
    {stop, Reason, State#state{pid=undefined}}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%------------------------------------------------------------------------
%%% Internal functions
%%%------------------------------------------------------------------------

do_onfailure(Reason, #state{onfailure=OnFailure} = State) ->
    case {Reason, OnFailure} of
    {reschedule,  _} -> reschedule;
    {restart,     _} -> restart;
    {normal,      _} -> reschedule;
    {shutdown,    _} -> shutdown;
    {_, Fun} when not is_tuple(Fun), is_function(Fun, 4) ->
        Stats = {State#state.restarts+1, State#state.intensity},
        try
            Fun(State#state.name, State#state.mfa, Reason, Stats)
        catch _:Err ->
            error_logger:error_msg("~w ~w onfailure callback error: ~80p\n",
                [?MODULE, self(), Err]),
            default
        end;
    {_, reschedule } -> reschedule;
    {_, shutdown   } -> shutdown;
    {_, {exit, Why}} -> {exit, Why};
    _                -> default
    end.

validate_sup_opts([{schedule, Sched} | Tail], State) ->
    validate_sup_opts(Tail, State#state{schedule=Sched});
validate_sup_opts([{restart, {MaxR, MaxT, Delay}} | Tail], State) ->
    if is_integer(MaxR), MaxR >= 0 -> ok;
    true -> throw({invalid_intensity, MaxR})
    end,
    if is_integer(MaxT), MaxT > 0 -> ok;
    true -> throw({invalid_period, MaxT})
    end,
    if is_integer(Delay), Delay >= 0 -> ok;
    true -> throw({invalid_delay, Delay})
    end,
    validate_sup_opts(Tail, State#state{intensity=MaxR, period=MaxT, delay=Delay});
validate_sup_opts([{onfailure, Fun} | Tail], State)
    when is_function(Fun,4); Fun =:= reschedule; Fun =:= exit; Fun =:= default
       ; is_tuple(Fun), element(1, Fun) =:= exit
      -> validate_sup_opts(Tail, State#state{onfailure=Fun});
validate_sup_opts([use_monitor | Tail], State) ->
    validate_sup_opts(Tail, State#state{use_monitor=true});
validate_sup_opts([{use_monitor, B} | Tail], State) when is_boolean(B) ->
    validate_sup_opts(Tail, State#state{use_monitor=B});
validate_sup_opts([{report_type, Type}| Tail], State) ->
    validate_sup_opts(Tail, State#state{report_type=Type});
validate_sup_opts([{shutdown, Type}| Tail], State) when Type=:=brutal_kill; is_integer(Type), Type>=0 ->
    validate_sup_opts(Tail, State#state{shutdown=Type});
validate_sup_opts([{onfailure, Other} | _], _) ->
    throw(?FMT("Invalid onfailure option ~w", [Other]));
validate_sup_opts([Other | _Tail], _) ->
    throw({unsupported_option, Other});
validate_sup_opts([], State) ->
    State.

-define(MAX_INT, 3600).

wakeup_interval(Secs, _MSecs) when Secs > ?MAX_INT -> ?MAX_INT*1000;
wakeup_interval(Secs,  MSecs) -> Secs*1000 - MSecs.

%% @spec (State::#state{}, Type) -> {ok, Status, State} | {error, Reason}
%%          Type = start | restart | reschedule | scheduled_stop
%%          Status = started | already_running | scheduled
start_child(#state{pid=Pid} = State, Type) ->
    Now         = now(),
    {Date,Time} = calendar:now_to_universal_time(Now),
    MSecs       = msecs(Now),
    When        = case Type of
                  reschedule -> get_following_time(State#state.schedule, {Date,Time});
                  _          -> get_next_time(State#state.schedule, {Date,Time})
                  end,
    cancel_timer(State#state.start_timer),
    case {When, is_child_alive(Pid)} of
    {{0, 0, {_FromTime, UntilTime}}, true} ->
        case State#state.until_time of
        UntilTime ->
            TRef = State#state.stop_timer;
        _ ->
            cancel_timer(State#state.stop_timer),
            Now  = now(),
            Int  = get_interval(now_to_time(Now), UntilTime, 1)*1000 - msecs(Now),
            TRef = erlang:send_after(Int, self(), {timer, scheduled_stop})
        end,
        % The pid is already running - nothing to do
        NewState = State#state{status=running, next_run=now, until_time=UntilTime, stop_timer=TRef},
        {ok, already_running, NewState};
    {{0, 0, {_FromTime, UntilTime}}, false} ->
        % Time to run now up until UntilTime
        try_start_child(State#state{next_run=now, until_time=UntilTime, last_start=Now}, UntilTime);
    {{Days, IntervalSecs, {StartTime, EndTime}}, IsChildAlive} ->
        stop_child(State, scheduled_start, normal, IsChildAlive),
        RunDate = calendar:gregorian_days_to_date(calendar:date_to_gregorian_days(Date) + Days),
        Wakeup  = wakeup_interval(IntervalSecs, MSecs),
        Report  = [{scheduled, State#state.mfa},
                   {next_run, to_string(RunDate, StartTime, EndTime)},
                   {wakeup_sec, Wakeup div 1000}],
        report_progress(State#state.reported, State#state.name, Report, State#state.report_type),
        TRef = erlang:send_after(Wakeup, self(), {timer, restart}),
        {ok, scheduled, State#state{start_timer=TRef, pid=undefined, reported=true, status=scheduled,
                                    next_run={RunDate, StartTime}, until_time=EndTime, monref=undefined}}
    end.

try_start_child(#state{mfa = {M,F,A}, name=Name, report_type=Type} = State, UntilTime) ->
    try
        ChildPid =
            case apply(M, F, A) of
            {ok, Pid} when is_pid(Pid) -> Pid;
            {error, Reason}            -> throw({cannot_start_child, Reason});
            Other                      -> throw({bad_return_value, Other})
            end,
        MRef =
            case State#state.use_monitor of
            true  ->
                Ref = erlang:monitor(process, ChildPid),
                unlink(ChildPid),
                Ref;
            false ->
                link(ChildPid),  % Just in case.
                undefined
            end,
        Report = [{started, [{pid, ChildPid}, {mfa, {M,F,A}}]}, {run_until, format(UntilTime)}],
        report_progress(false, Name, Report, Type),
        cancel_timer(State#state.start_timer),
        cancel_timer(State#state.stop_timer),
        Now      = now(),
        Interval = get_interval(now_to_time(Now), UntilTime, 1)*1000 - msecs(Now),
        TRef     = erlang:send_after(Interval, self(), {timer, scheduled_stop}),
        NewState = State#state{
             pid=ChildPid, start_timer=undefined, stop_timer=TRef, reported=false, 
             monref=MRef, status=running
        },
        {ok, started, NewState}
    catch _:Error ->
        {error, Error}
    end.

stop_child(#state{pid=Pid} = State, ReportReason, ExitReason) ->
    stop_child(State, ReportReason, ExitReason, is_child_alive(Pid)).

stop_child(#state{pid=Pid, mfa=MFA, name=SupName, report_type=Type} = State, ReportReason, Reason, true) ->
    report_shutdown(Pid, MFA, SupName, ReportReason, Type),
    case monitor_child(Pid, State#state.monref) of
    {ok, MonRef} ->
        case State#state.shutdown of
        brutal_kill  -> ShutdownTime = 0;
        ShutdownTime -> exit(Pid, Reason)  % Try to shutdown gracefully
        end,
        %
        receive
        {'DOWN', MonRef, process, Pid, _Reason} ->
            ok
        after ShutdownTime ->
            exit(Pid, kill),  % Force termination.
            receive
            {'DOWN', MonRef, process, Pid, _Reason} -> ok
            end
        end;
    {error, _Why} ->
        ok
    end;
stop_child(_State, _ReportReason, _Reason, false) ->
    ok.

is_child_alive(undefined) ->
    false;
is_child_alive(Pid) when is_pid(Pid) ->
    case {node(Pid), node()} of
    {N, N} -> erlang:is_process_alive(Pid);
    {N, _} ->
        case rpc:call(N, erlang, is_process_alive, [Pid]) of
        {badrpc, _Reason} ->
            false;
        Bool ->
            Bool
        end
    end.

%% Help function to stop_child/3 switches from link to monitor approach
monitor_child(_Pid, MonRef) when is_reference(MonRef) ->
    {ok, MonRef};
monitor_child(Pid, undefined) ->
    %% Do the monitor operation first so that if the child dies
    %% before the monitoring is done causing a 'DOWN'-message with
    %% reason noproc, we will get the real reason in the 'EXIT'-message
    %% unless a naughty child has already done unlink...
    MonRef = erlang:monitor(process, Pid),
    unlink(Pid),
    receive
	%% If the child dies before the unlink we must empty
	%% the mail-box of the 'EXIT'-message and the 'DOWN'-message.
	{'EXIT', Pid, Reason} ->
	    receive
		{'DOWN', _, process, Pid, _} ->
		    {error, Reason}
	    end
    after 0 ->
	    %% If a naughty child did unlink and the child dies before
	    %% monitor the result will be that shutdown/2 receives a
	    %% 'DOWN'-message with reason noproc.
	    %% If the child should die after the unlink there
	    %% will be a 'DOWN'-message with a correct reason
	    %% that will be handled in shutdown/2.
	    {ok, MonRef}
    end.


format({H,M,S}) ->
    i2l(H,2)++[$:]++i2l(M,2)++[$:]++i2l(S,2).

i2l(I,W) ->
    string:right(integer_to_list(I), W, $0).

supname(self, Mod) -> {self(),Mod};
supname(N, _)      -> N.

in_period(undefined, _Now, _Period) ->
    false;
in_period(Time, Now, Period) ->
    difference(Time, Now) =< Period.

difference({CurM, TimeS, _}, {CurM, CurS, _}) ->
    CurS - TimeS;
difference({TimeM, TimeS, _}, {CurM, CurS, _}) ->
    ((CurM - TimeM) * 1000000) + (CurS - TimeS).

cancel_timer(Ref) when is_reference(Ref) ->
    erlang:cancel_timer(Ref);
cancel_timer(_) ->
    ok.

%%-------------------------------------------------------------------------
%% Progress reports
%%-------------------------------------------------------------------------
report_progress(_AlreadyReported = true, _SupName, _Opts, _ReportType) ->
    ok;
report_progress(_AlreadyReported = false, SupName, Opts, ReportType) ->
    Progress = [{supervisor, SupName}] ++ Opts,
    do_report(ReportType, info_report, progress, Progress).

report_shutdown(Pid, MFA, SupName, Reason, Type) ->
    Progress = [{supervisor, SupName},
                {stopped, [{pid, Pid}, {mfa, MFA}]}, {reason, Reason}],
    do_report(Type, info_report, progress, Progress).

report_error(Error, Reason, #state{name = Name, pid = Pid, mfa = MFA, report_type=Type}) ->
    ErrorMsg = [{supervisor, Name},
                {errorContext, Error},
                {reason, Reason},
                {offender, [{pid, Pid}, {mfa, MFA}]}],
    do_report(Type, error_report, supervisor_report, ErrorMsg).

do_report(undefined, F, Report, Msg) ->
    error_logger:F(Report, Msg);
do_report(Other, F, Report, Msg) ->
    error_logger:F({Other, Report}, Msg).

%%-------------------------------------------------------------------------
%% @spec (DaySpecs::schedule()) -> {'$timespec$', Result::timespec()}
%% @doc Validate day/time specification. Thow error on invalid specification
%%      format.
%% @end
%%-------------------------------------------------------------------------
validate_schedule({?COMPILED_SPEC, DaySpecs}=S) when is_tuple(DaySpecs), tuple_size(DaySpecs) =:= 7 ->
    S;
validate_schedule(DaySpecs) when is_tuple(DaySpecs), tuple_size(DaySpecs) =:= 7 -> 
    {?COMPILED_SPEC, DaySpecs};
validate_schedule(DaySpecs) ->
    {?COMPILED_SPEC, validate_time_spec(DaySpecs, erlang:make_tuple(7, []))}.

validate_time_spec([DaySpec | Tail], Days) ->
    NewDays = validate_day_spec(DaySpec, Days),
    validate_time_spec(Tail, NewDays);
validate_time_spec([], Days) ->
    Days.

validate_day_spec({any, TimeInt}, Days) ->
    Time = validate_time_intervals(TimeInt, []),
    lists:foldl(fun(Day, Acc) -> append_time(Day, Acc, Time) end, Days, [1,2,3,4,5,6,7]);
validate_day_spec({DayList, TimeInt}, Days) when is_list(DayList) ->
    Time = validate_time_intervals(TimeInt, []),
    lists:foldl(fun(Day, Acc) -> validate_day_spec2(Day, Acc, Time) end, Days, DayList);
validate_day_spec({Day, TimeInt}, Days) when is_list(TimeInt) ->
    Time = validate_time_intervals(TimeInt, []),
    validate_day_spec2(Day, Days, Time);
validate_day_spec(Other, _Days) ->
    throw({error, ?FMT("Invalid day spec: ~w", [Other])}).

validate_day_spec2(Day, Days, Time) ->
    IntDay = decode_day(Day),
    append_time(IntDay, Days, Time).

append_time(I, Days, TimeL2) ->
    TimeL1 = element(I, Days),
    Time = lists:keymerge(1, TimeL1, TimeL2),
    setelement(I, Days, Time).

validate_time_intervals([Interval | Tail], Acc) ->
    I = validate_time_interval(Interval),
    validate_time_intervals(Tail, I ++ Acc);
validate_time_intervals([], L) ->
    Res = lists:sort(L),
    case [{{A,B},{C,D}} || {A,B} = X1 <- Res, {C,D} = X2 <- Res, X1 < X2, A =< D, C =< B] of
    []    -> Res;
    Cross -> throw({error, ?FMT("Found crossover times: ~w", [Cross])})
    end.

validate_time_interval({From, To}) when is_list(From), is_list(To) ->
    validate_time_interval({parse_time(From), parse_time(To)});
validate_time_interval({From, To}) when is_tuple(From), is_list(To) ->
    validate_time_interval({From, parse_time(To)});
validate_time_interval({From, To}) when is_list(From), is_tuple(To) ->
    validate_time_interval({parse_time(From), To});
validate_time_interval({From = {H1,M1,S1}, To = {H2,M2,S2}} = T)
    when is_integer(H1), H1 >= 0, H1 < 24
       , is_integer(M1), M1 >= 0, M1 < 60
       , is_integer(S1), S1 >= 0, S1 < 60
       , is_integer(H2), H2 >= 0, H2 < 24
       , is_integer(M2), M2 >= 0, M2 < 60
       , is_integer(S2), S2 >= 0, S2 < 60
       , From < To
->
    [T];
validate_time_interval([]) ->
    [];
validate_time_interval(Other) ->
    throw({error, ?FMT("Invalid time spec: ~200p", [Other])}).

parse_time(Time) ->
    try
        case T = list_to_tuple([list_to_integer(L) || L <- string:tokens(Time, ":")]) of
        {_,_,_} -> T;
        {H,M}   -> {H,M,0}
        end
    catch _:_ ->
        throw({error, ?FMT("Invalid time spec: ~p", [Time])})
    end.

decode_day(mon)  -> 1;
decode_day(tue)  -> 2;
decode_day(wed)  -> 3;
decode_day(thu)  -> 4;
decode_day(fri)  -> 5;
decode_day(sat)  -> 6;
decode_day(sun)  -> 7;
decode_day(N)    -> throw({error, ?FMT("Invalid day spec: ~w", [N])}).

%%-------------------------------------------------------------------------
%% @spec (DayOfWeek::integer()) -> Result::string()
%% @doc Converts a day of week to a string. Special case: when argument
%%      is 0, it returns "Today".
%% @end
%%-------------------------------------------------------------------------
dow_to_string(0) -> "Today";
dow_to_string(1) -> "Mon";
dow_to_string(2) -> "Tue";
dow_to_string(3) -> "Wed";
dow_to_string(4) -> "Thu";
dow_to_string(5) -> "Fri";
dow_to_string(6) -> "Sat";
dow_to_string(7) -> "Sun";
dow_to_string(N) -> dow_to_string(N rem 7).

to_string({Y,M,D} = Date, {H1,M1,S1}, {H2,M2,S2}) ->
    DOW = dow_to_string(calendar:day_of_the_week(Date)),
    ?FMT("~s ~w/~.2.0w/~.2.0w [~.2.0w:~.2.0w:~.2.0w - ~.2.0w:~.2.0w:~.2.0w]",
         [DOW, Y,M,D, H1,M1,S1, H2,M2,S2]).

to_string([Dow|_] = DOWs, Times) when is_integer(Dow) ->
    Dows = string:join([dow_to_string(I) || I <- DOWs], ","),
    Tms  = string:join([io_lib:format("~.2.0w:~.2.0w:~.2.0w-~.2.0w:~.2.0w:~.2.0w",
                        [H1,M1,S1, H2,M2,S2]) || {{H1,M1,S1}, {H2,M2,S2}} <- Times],
                       ","),
    ?FMT("~s [~s]", [Dows, Tms]);
to_string([], _) ->
    [].

%%-------------------------------------------------------------------------
%% @spec (TimeSpec::timespec()) -> Result::string()
%% @doc Converts a validated timespec() into a printable string.
%% @end
%%-------------------------------------------------------------------------
print_timespec({?COMPILED_SPEC, Spec}) ->
    print_timespec(Spec);
print_timespec(Spec) when is_tuple(Spec), tuple_size(Spec) =:= 7 ->
    Specs = merge_specs(tuple_to_list(Spec), 1, {1, undefined}, []),
    string:join(Specs, "; ").

merge_specs([undefined | T], N, {I, S}, Acc) ->
    merge_specs(T, N+1, {I, S}, Acc);
merge_specs([Spec | T], N, {_I, undefined}, Acc) ->
    merge_specs(T, N+1, {N, Spec}, [N|Acc]);
merge_specs([Spec | T], N, {I, Spec}, Acc) ->
    merge_specs(T, N+1, {I, Spec}, [N|Acc]);
merge_specs([Spec | T], N, {I, S}, []) ->
    [to_string([I], S) | merge_specs(T, N+1, {N, Spec}, [N])];
merge_specs([undefined | T], N, {I, S}, Acc) ->
    merge_specs(T, N+1, {I, S}, Acc);
merge_specs([Spec | T], N, {_I, S}, Acc) ->
    [to_string(lists:reverse(Acc), S) | merge_specs(T, N+1, {N, Spec}, [])];
merge_specs([], _N, {_I, undefined}, []) ->
    [];
merge_specs([], _N, {I, S}, Acc) ->
    [to_string(lists:reverse([I|Acc]), S)].

%%-------------------------------------------------------------------------
%% @equiv get_next_time/2
%% @doc Function returns execution time following next execution time.
%%      E.g. if the scheduling window is 
%%      `[{mon, {"9:00:00", "17:00:00"}}, {tue, {"9:00:00", "17:00:00"}}]'
%%      and the function is called at 10am on Monday, it'll return Tuesday
%%      9am time (`{1, IntervalSeconds, {{9,0,0},{17,0,0}}}').
%% @end
%%-------------------------------------------------------------------------
get_following_time(Sched, {Date, Time}) when is_tuple(Sched), tuple_size(Sched) =:= 7 ->
    DOW = calendar:day_of_the_week(Date),
    get_next_time(Sched, DOW, Time, 0, {Date, Time}, skip_current);
get_following_time({?COMPILED_SPEC, Spec}, DateTime) ->
    get_following_time(Spec, DateTime).

%%-------------------------------------------------------------------------
%% @spec (Sched::timemask(), {Date, Time}) -> Result
%%       Result = {Days::integer(), IntervalSeconds::integer(),
%%                      {StartTime::time(), EndTime::time()}}
%% @doc Function returns next execution time.
%%      `IntervalSeconds' is the total number of seconds until next run.
%%      `Days' is the number of days of week between `Date' and date of
%%      next run.
%% @end
%%-------------------------------------------------------------------------
get_next_time(Sched, {Date, Time}) when is_tuple(Sched), tuple_size(Sched) =:= 7 ->
    DOW = calendar:day_of_the_week(Date),
    get_next_time(Sched, DOW, Time, 0, {Date, Time}, undefined);
get_next_time({?COMPILED_SPEC, Spec}, DateTime) ->
    get_next_time(Spec, DateTime).

get_next_time(Sched, DOW, Time, N, NowDateTime, SkipCurrent) when N < 8 ->
    case element(DOW, Sched) of
    Times when is_list(Times) ->
        case get_next_time2(N, Times, Time, SkipCurrent) of
        false ->
            get_next_time(Sched, ((DOW-1 + 1) rem 7) + 1, {0,0,0}, N+1, NowDateTime, undefined);
        {0, Secs, UntilTime} ->
            {0, Secs, UntilTime};
        {Days, Secs, RunDateTime} ->
            {_Dt, Tm} = NowDateTime,
            SecsTillNextDay = 86400 - calendar:time_to_seconds(Tm),
            TotalSecs = (Days-1)*86400 + SecsTillNextDay + Secs,
            {Days, TotalSecs, RunDateTime}
        end;
    undefined ->
        get_next_time(Sched, ((DOW-1 + 1) rem 7) + 1, {0,0,0}, N+1, NowDateTime, undefined)
    end;
get_next_time(_, _, _, _, _, _) ->
    throw({error, "No runnable time interval found!"}).

%% [FromTime ..... ToTime]
%%             ^
%%            Now
get_next_time2(Days, [{From, To} | _Tail], Time, undefined) when From =< Time, Time =< To ->
    case Days of
    0 -> {Days, 0, {From, To}};
    _ -> {Days, 0, {From, To}}
    end;
%% [... ToTime]
%%              ^
%%             Now
get_next_time2(_Days, [{_From, To}], Time, _) when To < Time ->
    false;
%%   [FromTime ...]
%% ^
%% Now
get_next_time2(Days, [{From, To} | _], Time, _) when Time < From ->
    {Days, get_interval(Time, From, 0), {From, To}};
get_next_time2(Days, [_ | Tail], Time, SkipCurrent) ->
    get_next_time2(Days, Tail, Time, SkipCurrent);
get_next_time2(_, [], _Time, _) ->
    false.

get_interval(NowTime, ToTime, Add) when NowTime =< ToTime ->
    ToSeconds  = calendar:time_to_seconds(ToTime),
    NowSeconds = calendar:time_to_seconds(NowTime),
    ToSeconds - NowSeconds + Add.

msecs(_Now = {_, _, MkSecs}) ->
    MkSecs div 1000.
now_to_time(Now) ->
    element(2, calendar:now_to_universal_time(Now)).

%% @private
test_mfa(Name, Delay, ExitReason) ->
    Pid = spawn_link(fun() ->
            process_flag(trap_exit, true),
            Parent = case element(2, process_info(self(), links)) of
                     []  -> undefined;
                     [P] -> P
                     end,
            error_logger:info_msg("~w: Started process ~w\n", [Name, self()]),
            receive
            {'EXIT', Pid, Why} when Pid =:= Parent ->
                error_logger:info_msg("~w: killed by linked parent with reason: ~w\n", [Name, Why]),
                exit(Why)
            after Delay ->
                error_logger:info_msg("~w: Ending process ~w with reason: ~w\n", [Name, self(), ExitReason]),
                exit(ExitReason)
            end
          end),
    {ok, Pid}.

%% @private
test() ->
    application:start(sasl),
    T=calendar:time_to_seconds(time()),
    FT1=calendar:seconds_to_time(T+3),
    TT1=calendar:seconds_to_time(T+8),
    FT2=calendar:seconds_to_time(T+15),
    TT2=calendar:seconds_to_time(T+20),
    Schedule = [{[mon,tue,wed,thu,fri,sat,sun], [{FT1, TT1},{FT2, TT2}]}],
    timed_supervisor:start_link({timed_supervisor, test_mfa, [one, 7000, normal]}, [{schedule, Schedule}]),
    timed_supervisor:start_link({timed_supervisor, test_mfa, [two, 20000, normal]}, []),
    ok.

%% f(P), {ok, P} = timed_supervisor:start({timed_supervisor, test_mfa, [one, 3000, normal]}, [{schedule, [{any, [{"9:00", "23:30"}]}]}, {restart, {2, 20, 3}}, {onfailure, fun(_,_,_,{I,N}) -> if I>N -> {exit, normal}; true -> default end end}]).
