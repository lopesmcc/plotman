import curses
import datetime
import locale
import math
import os
import subprocess
import shlex
import time
import sys

from plotman import archive, configuration, manager, reporting
from plotman.job import Job


ON_POSIX = 'posix' in sys.builtin_module_names


class TerminalTooSmallError(Exception):
    pass

class Log:
    entries: typing.List[str]
    cur_pos: int

    def __init__(self) -> None:
        self.entries = []
        self.cur_pos = 0

    # TODO: store timestamp as actual timestamp indexing the messages
    def log(self, msg: str) -> None:
        '''Log the message and scroll to the end of the log'''
        ts = datetime.datetime.now().strftime('%m-%d %H:%M:%S')
        self.entries.append(ts + ' ' + msg)
        self.cur_pos = len(self.entries)

    def tail(self, num_entries: int) -> typing.List[str]:
        '''Return the entries at the end of the log.  Consider cur_slice() instead.'''
        return self.entries[-num_entries:]

    def shift_slice(self, offset: int) -> None:
        '''Positive shifts towards end, negative shifts towards beginning'''
        self.cur_pos = max(0, min(len(self.entries), self.cur_pos + offset))

    def shift_slice_to_end(self) -> None:
        self.cur_pos = len(self.entries)

    def get_cur_pos(self) -> int:
        return self.cur_pos

    def cur_slice(self, num_entries: int) -> typing.List[str]:
        '''Return num_entries log entries up to the current slice position'''
        return self.entries[max(0, self.cur_pos - num_entries) : self.cur_pos]

    def fill_log(self) -> None:
        '''Add a bunch of stuff to the log.  Useful for testing.'''
        for i in range(100):
            self.log('Log line %d' % i)

def plotting_status_msg(active: bool, status: str) -> str:
    if active:
        return '(active) ' + status
    else:
        return '(inactive) ' + status

def archiving_status_msg(configured: bool, active: bool, status: str) -> str:
    if configured:
        if active:
            return '(active) ' + status
        else:
            return '(inactive) ' + status
    else:
        return '(not configured)'

# cmd_autostart_plotting is the (optional) argument passed from the command line. May be None
def curses_main(stdscr: typing.Any, cmd_autostart_plotting: typing.Optional[bool], cmd_autostart_archiving: typing.Optional[bool], cfg: configuration.PlotmanConfig) -> None:
    log = Log()

    if should_use_external_plotting(cfg):
        plotting_active = False
    elif cmd_autostart_plotting is not None:
        plotting_active = cmd_autostart_plotting
    else:
        plotting_active = cfg.commands.interactive.autostart_plotting

    archiving_configured = cfg.archiving is not None

    if not archiving_configured or should_use_external_plotting(cfg):
        archiving_active = False
    elif cmd_autostart_archiving is not None:
        archiving_active = cmd_autostart_archiving
    else:
        archiving_active = cfg.commands.interactive.autostart_archiving

    plotting_status = '<startup>'    # todo rename these msg?
    archiving_status: typing.Union[bool, str, typing.Dict[str, object]] = '<startup>'

    curses.start_color()
    stdscr.nodelay(True)  # make getch() non-blocking
    stdscr.timeout(2000)

    # Create windows.  We'll size them in the main loop when we have their content.
    header_win = curses.newwin(1, 1, 1, 0)
    log_win = curses.newwin(1, 1, 1, 0)
    jobs_win = curses.newwin(1, 1, 1, 0)
    dirs_win = curses.newwin(1, 1, 1, 0)

    jobs = Job.get_running_jobs(cfg.logging.plots)
    last_refresh = None

    pressed_key = ''   # For debugging

    archdir_freebytes = None
    aging_reason = None

    while True:
        # A full refresh scans for and reads info for running jobs from
        # scratch (i.e., reread their logfiles).  Otherwise we'll only
        # initialize new jobs, and mostly rely on cached info.
        do_full_refresh = False
        elapsed = 0    # Time since last refresh, or zero if no prev. refresh
        if last_refresh is None:
            do_full_refresh = True
        else:
            elapsed = (datetime.datetime.now() - last_refresh).total_seconds()
            do_full_refresh = elapsed >= cfg.scheduling.polling_time_s

        if not do_full_refresh:
            jobs = Job.get_running_jobs(cfg.logging.plots, cached_jobs=jobs)

        else:
            last_refresh = datetime.datetime.now()
            jobs = Job.get_running_jobs(cfg.logging.plots)

            if plotting_active or is_external_plotting_active(cfg):
                (started, msg) = manager.maybe_start_new_plot(
                    cfg.directories, cfg.scheduling, cfg.plotting, should_use_external_plotting(cfg)
                )
                if (started):
                    if not should_use_external_plotting(cfg):
                        if aging_reason is not None:
                            log.log(aging_reason)
                            aging_reason = None
                        log.log(msg)
                    plotting_status = '<just started job>'
                    jobs = Job.get_running_jobs(cfg.logging.plots, cached_jobs=jobs)
                else:
                    # If a plot is delayed for any reason other than stagger, log it
                    if msg.find("stagger") < 0:
                        aging_reason = msg
                    plotting_status = msg

            if archiving_configured:
                if archiving_active or is_external_archiving_active(cfg):
                    archiving_status, log_messages = archive.spawn_archive_process(cfg.directories, cfg.archiving, cfg.logging, jobs, should_use_external_archiver(cfg))
                    for log_message in log_messages:
                        log.log(log_message)

                archdir_freebytes, log_messages = archive.get_archdir_freebytes(cfg.archiving)
                for log_message in log_messages:
                    log.log(log_message)


        # Get terminal size.  Recommended method is stdscr.getmaxyx(), but this
        # does not seem to work on some systems.  It may be a bug in Python
        # curses, maybe having to do with registering sigwinch handlers in
        # multithreaded environments.  See e.g.
        #     https://stackoverflow.com/questions/33906183#33906270
        # Alternative option is to call out to `stty size`.  For now, we
        # support both strategies, selected by a config option.
        # TODO: also try shutil.get_terminal_size()
        n_rows: int
        n_cols: int
        if cfg.user_interface.use_stty_size:
            completed_process = subprocess.run(
                ['stty', 'size'], check=True, encoding='utf-8', stdout=subprocess.PIPE
            )
            elements = completed_process.stdout.split()
            (n_rows, n_cols) = [int(v) for v in elements]
        else:
            (n_rows, n_cols) = map(int, stdscr.getmaxyx())

        stdscr.clear()
        stdscr.resize(n_rows, n_cols)
        curses.resize_term(n_rows, n_cols)

        #
        # Obtain and measure content
        #

        # Directory prefixes, for abbreviation
        tmp_prefix = os.path.commonpath(cfg.directories.tmp)
        dst_dir = cfg.directories.get_dst_directories()
        dst_prefix = os.path.commonpath(dst_dir)
        if archdir_freebytes is not None:
            archive_directories = list(archdir_freebytes.keys())
            if len(archive_directories) == 0:
                arch_prefix = ''
            else:
                arch_prefix = os.path.commonpath(archive_directories)

        n_tmpdirs = len(cfg.directories.tmp)

        # Directory reports.
        tmp_report = reporting.tmp_dir_report(
            jobs, cfg.directories, cfg.scheduling, n_cols, 0, n_tmpdirs, tmp_prefix)
        dst_report = reporting.dst_dir_report(
            jobs, dst_dir, n_cols, dst_prefix)
        if archdir_freebytes is not None:
            arch_report = reporting.arch_dir_report(archdir_freebytes, n_cols, arch_prefix)
            if not arch_report:
                arch_report = '<no archive dir info>'
        else:
            arch_report = '<archiving not configured>'

        #
        # Layout
        #

        tmp_h = len(tmp_report.splitlines())
        tmp_w = len(max(tmp_report.splitlines(), key=len)) + 1
        dst_h = len(dst_report.splitlines())
        dst_w = len(max(dst_report.splitlines(), key=len)) + 1
        arch_h = len(arch_report.splitlines()) + 1
        arch_w = n_cols

        header_h = 3
        dirs_h = max(tmp_h, dst_h) + arch_h
        remainder = n_rows - (header_h + dirs_h)
        jobs_h = max(5, math.floor(remainder * 0.6))

        header_pos = 0
        jobs_pos = header_pos + header_h
        stdscr.resize(n_rows, n_cols)
        dirs_pos = jobs_pos + jobs_h
        logscreen_pos = dirs_pos + dirs_h

        linecap = n_cols - 1
        if cfg.user_interface.show_logs:
            logs_h = n_rows - (header_h + jobs_h + dirs_h)
        else:
            logs_h = 0
            jobs_h = n_rows - (header_h + dirs_h)
            dirs_pos = jobs_pos + jobs_h

        try:
            header_win = curses.newwin(header_h, n_cols, header_pos, 0)
            if cfg.user_interface.show_logs:
                log_win = curses.newwin(logs_h, n_cols, logscreen_pos, 0)
            jobs_win = curses.newwin(jobs_h, n_cols, jobs_pos, 0)
            dirs_win = curses.newwin(dirs_h, n_cols, dirs_pos, 0)
        except Exception:
            raise Exception('Failed to initialize curses windows, try a larger '
                            'terminal window.')

        #
        # Write
        #

        # Header
        curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_RED)
        curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)

        header_win.addnstr(0, 0, 'Plotman', linecap, curses.A_BOLD)
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        refresh_msg = "now" if do_full_refresh else f"{int(elapsed)}s/{cfg.scheduling.polling_time_s}"
        header_win.addnstr(f" {timestamp} (refresh {refresh_msg})", linecap)
        header_win.addnstr('  |  <P>lotting: ', linecap, curses.A_BOLD)
        if plotting_active or is_external_plotting_active(cfg):
            header_win.addnstr('(active)', linecap, curses.color_pair(2))
        else:
            header_win.addnstr('(inactive)', linecap, curses.color_pair(1) | curses.A_BOLD)
        header_win.addnstr(' ' + plotting_status, linecap)
        header_win.addnstr(' <A>rchival: ', linecap, curses.A_BOLD)

        if archiving_configured:
            if archiving_active or is_external_archiving_active(cfg):
                header_win.addnstr('(active)', linecap, curses.color_pair(2))
            else:
                header_win.addnstr('(inactive)', linecap, curses.color_pair(1) | curses.A_BOLD)
            header_win.addnstr(' ' + archiving_status, linecap)
        else:
            '(not configured)'

        # Oneliner progress display
        header_win.addnstr(1, 0, 'Jobs (%d): ' % len(jobs), linecap)
        header_win.addnstr('[' + reporting.job_viz(jobs) + ']', linecap)

        # These are useful for debugging.
        # header_win.addnstr('  term size: (%d, %d)' % (n_rows, n_cols), linecap)  # Debuggin
        # if pressed_key:
            # header_win.addnstr(' (keypress %s)' % str(pressed_key), linecap)
        header_win.addnstr(2, 0, 'Prefixes:', linecap, curses.A_BOLD)
        header_win.addnstr('  tmp=', linecap, curses.A_BOLD)
        header_win.addnstr(tmp_prefix, linecap)
        header_win.addnstr('  dst=', linecap, curses.A_BOLD)
        header_win.addnstr(dst_prefix, linecap)
        if archiving_configured:
            header_win.addnstr('  archive=', linecap, curses.A_BOLD)
            header_win.addnstr(arch_prefix, linecap)
        header_win.addnstr(' (remote)', linecap)


        # Jobs
        jobs_win.addstr(0, 0, reporting.status_report(jobs, n_cols, jobs_h,
            tmp_prefix, dst_prefix))
        jobs_win.chgat(0, 0, curses.A_REVERSE)

        # Dirs
        tmpwin_dstwin_gutter = 6

        maxtd_h = max([tmp_h, dst_h])

        tmpwin = curses.newwin(
                    tmp_h, tmp_w,
                    dirs_pos + int(maxtd_h - tmp_h), 0)
        tmpwin.addstr(tmp_report)
        tmpwin.chgat(0, 0, curses.A_REVERSE)

        dstwin = curses.newwin(
                dst_h, dst_w,
                dirs_pos + int((maxtd_h - dst_h) / 2), tmp_w + tmpwin_dstwin_gutter)
        dstwin.addstr(dst_report)
        dstwin.chgat(0, 0, curses.A_REVERSE)

        archwin = curses.newwin(arch_h, arch_w, dirs_pos + maxtd_h, 0)
        archwin.addstr(0, 0, 'Archive dirs free space', curses.A_REVERSE)
        archwin.addstr(1, 0, arch_report)

        if cfg.user_interface.show_logs:
            # Log.  Could use a pad here instead of managing scrolling ourselves, but
            # this seems easier.
            log_win.addnstr(0, 0, ('Log: %d (<up>/<down>/<end> to scroll)\n' % log.get_cur_pos() ),
                    linecap, curses.A_REVERSE)
            for i, logline in enumerate(log.cur_slice(logs_h - 1)):
                log_win.addnstr(i + 1, 0, logline, linecap)

        stdscr.noutrefresh()
        header_win.noutrefresh()
        jobs_win.noutrefresh()
        tmpwin.noutrefresh()
        dstwin.noutrefresh()
        archwin.noutrefresh()
        if cfg.user_interface.show_logs:
            log_win.noutrefresh()
        curses.doupdate()

        try:
            key = stdscr.getch()
        except KeyboardInterrupt:
            key = ord('q')

        if key == curses.KEY_UP:
            log.shift_slice(-1)
            pressed_key = 'up'
        elif key == curses.KEY_DOWN:
            log.shift_slice(1)
            pressed_key = 'dwn'
        elif key == curses.KEY_END:
            log.shift_slice_to_end()
            pressed_key = 'end'
        elif key == ord('p'):
            if should_use_external_plotting(cfg):
                toggle_external_plotter(cfg)
            else:
                plotting_active = not plotting_active
            pressed_key = 'p'
        elif key == ord('a'):
            if should_use_external_archiver(cfg):
                toggle_external_archiver(cfg)
            else:
                archiving_active = not archiving_active
            pressed_key = 'a'
        elif key == ord('q'):
            break
        else:
            pressed_key = key


def should_use_external_plotting(cfg):
    has_start_plotter_cmd = cfg.user_interface.start_plotter_cmd is not None
    has_stop_plotter_cmd = cfg.user_interface.stop_plotter_cmd is not None
    has_is_plotter_active_cmd = cfg.user_interface.is_plotter_active_cmd is not None
    if has_start_plotter_cmd and has_stop_plotter_cmd and has_is_plotter_active_cmd:
        return True
    if has_start_plotter_cmd or has_stop_plotter_cmd or has_is_plotter_active_cmd:
        raise Exception('Invalid configuration for the UI external plotter control: '
                        'all 3 fields are required to enable it.')
    return False


def is_external_plotting_active(cfg):
    if not should_use_external_plotting(cfg):
        return False
    cmd = shlex.split(cfg.user_interface.is_plotter_active_cmd)
    try:
        check_call(cmd, stdout=DEVNULL, stderr=STDOUT)
        return True
    except CalledProcessError as e:
        return False


def toggle_external_plotter(cfg):
    if is_external_plotting_active(cfg):
        cmd = shlex.split(cfg.user_interface.stop_plotter_cmd)
        check_call(cmd, stdout=DEVNULL, stderr=STDOUT)
    else:
        cmd = shlex.split(cfg.user_interface.start_plotter_cmd)
        check_call(cmd, stdout=DEVNULL, stderr=STDOUT)


def should_use_external_archiver(cfg):
    has_start_archiver_cmd = cfg.user_interface.start_archiver_cmd is not None
    has_stop_archiver_cmd = cfg.user_interface.stop_archiver_cmd is not None
    has_is_archiver_active_cmd = cfg.user_interface.is_archiver_active_cmd is not None
    if has_start_archiver_cmd and has_stop_archiver_cmd and has_is_archiver_active_cmd:
        return True
    if has_start_archiver_cmd or has_stop_archiver_cmd or has_is_archiver_active_cmd:
        raise Exception('Invalid configuration for the UI external archiver control: '
                        'all 3 fields are required to enable it.')
    return False


def is_external_archiving_active(cfg):
    if not should_use_external_archiver(cfg):
        return False
    cmd = shlex.split(cfg.user_interface.is_archiver_active_cmd)
    try:
        check_call(cmd, stdout=DEVNULL, stderr=STDOUT)
        return True
    except CalledProcessError as e:
        return False


def toggle_external_archiver(cfg):
    if is_external_archiving_active(cfg):
        cmd = shlex.split(cfg.user_interface.stop_archiver_cmd)
        check_call(cmd, stdout=DEVNULL, stderr=STDOUT)
    else:
        cmd = shlex.split(cfg.user_interface.start_archiver_cmd)
        check_call(cmd, stdout=DEVNULL, stderr=STDOUT)

def run_interactive(cfg: configuration.PlotmanConfig, autostart_plotting: typing.Optional[bool] = None, autostart_archiving: typing.Optional[bool] = None) -> None:
    locale.setlocale(locale.LC_ALL, '')
    code = locale.getpreferredencoding()
    # Then use code as the encoding for str.encode() calls.

    try:
        curses.wrapper(
            curses_main,
            cmd_autostart_plotting=autostart_plotting,
            cmd_autostart_archiving=autostart_archiving,
            cfg=cfg,
        )
    except curses.error as e:
        raise TerminalTooSmallError(
            "Your terminal may be too small, try making it bigger.",
        ) from e
