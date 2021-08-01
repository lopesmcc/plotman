import curses
import datetime
import locale
import math
import os
import subprocess
import shlex
import typing
import sys
import reporting
import archive
from job import Job

ON_POSIX = 'posix' in sys.builtin_module_names

class TerminalTooSmallError(Exception):
    pass


def curses_main(stdscr: typing.Any, cfg: configuration.PlotmanConfig) -> None:
    curses.start_color()
    stdscr.nodelay(True)
    stdscr.timeout(2000)

    jobs = []
    last_refresh = None
    archdir_freebytes = None

    while True:
        # A full refresh scans for and reads info for running jobs from
        # scratch (i.e., reread their logfiles).  Otherwise we'll only
        # initialize new jobs, and mostly rely on cached info.
        elapsed = 0    # Time since last refresh, or zero if no prev. refresh
        if last_refresh is None:
            do_full_refresh = True
        else:
            elapsed = (datetime.datetime.now() - last_refresh).total_seconds()
            do_full_refresh = elapsed >= POLLING_TIME_S

        if do_full_refresh:
            last_refresh = datetime.datetime.now()

            archdir_freebytes, _ = archive.get_archdir_freebytes(cfg.archiving)
            if archdir_freebytes is not None:
                archive_directories = list(archdir_freebytes.keys())
                if len(archive_directories) > 0:
                    farm_path = os.path.commonpath(archive_directories)
                    jobs = Job.get_running_jobs(farm_path=farm_path, prev_jobs=jobs)

        n_rows: int
        n_cols: int
        completed_process = subprocess.run(
            ['stty', 'size'], check=True, encoding='utf-8', stdout=subprocess.PIPE
        )
        elements = completed_process.stdout.split()
        (n_rows, n_cols) = [int(v) for v in elements]

        stdscr.clear()
        stdscr.resize(n_rows, n_cols)
        curses.resize_term(n_rows, n_cols)

        if archdir_freebytes is not None:
            archive_directories = list(archdir_freebytes.keys())
            if len(archive_directories) == 0:
                arch_prefix = ''
            else:
                arch_prefix = os.path.commonpath(archive_directories)

        # Directory reports.
        arch_report = reporting.arch_dir_report(archdir_freebytes, n_cols, arch_prefix)

        # Layout
        arch_h = len(arch_report.splitlines()) + 1
        arch_w = n_cols

        header_h = 3
        header_pos = 0

        jobs_pos = header_pos + header_h
        stdscr.resize(n_rows, n_cols)

        linecap = n_cols - 1
        jobs_h = n_rows - (header_h + arch_h)
        dirs_pos = jobs_pos + jobs_h

        try:
            header_win = curses.newwin(header_h, n_cols, header_pos, 0)
            jobs_win = curses.newwin(jobs_h, n_cols, jobs_pos, 0)
        except Exception:
            raise Exception('Failed to initialize curses windows, try a larger '
                            'terminal window.')

        # Header
        curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_RED)
        curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)

        header_win.addnstr(0, 0, 'Archiving Monitor', linecap, curses.A_BOLD)
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        refresh_msg = "now" if do_full_refresh else f"{int(elapsed)}s/{POLLING_TIME_S}"
        header_win.addnstr(f" {timestamp} (refresh {refresh_msg})", linecap)

        header_win.addnstr('  |  Jobs: ', linecap, curses.A_BOLD)
        header_win.addnstr('%d' % len(jobs), linecap, curses.color_pair(2))

        # Oneliner progress display
        header_win.addnstr(1, 0, 'Jobs (%d): ' % len(jobs), linecap)
        header_win.addnstr('[' + reporting.job_viz(jobs) + ']', linecap)

        # Jobs
        jobs_win.addstr(0, 0, reporting.archive_status_report(jobs, n_cols, jobs_h))
        jobs_win.chgat(0, 0, curses.A_REVERSE)

        arch_win = curses.newwin(arch_h, arch_w, dirs_pos, 0)
        arch_win.addstr(0, 0, 'Archive dirs', curses.A_REVERSE)
        arch_win.addstr(1, 0, arch_report)

        stdscr.noutrefresh()
        header_win.noutrefresh()
        jobs_win.noutrefresh()
        arch_win.noutrefresh()

        curses.doupdate()

        try:
            key = stdscr.getch()
        except KeyboardInterrupt:
            key = ord('q')

        if key == ord('q'):
            break


def run_monitor(cfg: configuration.PlotmanConfig) -> None:
    locale.setlocale(locale.LC_ALL, '')

    try:
        curses.wrapper(
            curses_main,
            cfg=cfg
        )
    except curses.error as e:
        raise TerminalTooSmallError(
            "Your terminal may be too small, try making it bigger.",
        ) from e


