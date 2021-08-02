import os
import re
import subprocess
from datetime import datetime
from typing import Tuple, List, Optional

from plotman import plot_util, configuration, archive


class EgressArchiveJob:
    plot_id: str
    plot_k: int
    plot_timestamp: float
    source_disk: str
    dest_disk: str
    start_timestamp: float
    bw_limit: int


    @classmethod
    def get_archive_running_jobs(cls, arch_cfg: configuration.Archiving) -> List["EgressArchiveJob"]:
        procs = archive.get_running_archive_jobs(arch_cfg)
        jobs = []
        for proc in procs:
            start_timestamp = proc.create_time()
            split = proc.cmdline()
            if len(split) != 9:
                continue
            plot_path = split[7]
            dest_disk = split[8]
            matches = re.search(r"^rsync://\S+@([\w\d]+):\d+/[\w\d]+/(\d+)/$", dest_disk)
            if matches is not None:
                groups = matches.groups()
                dest_disk = f"/{groups[1]}@{groups[0]}"

            bw_limit = int(split[1].split('=')[1])
            matches = re.search(r"^([/\w\d]+)/plot-k(\d{2})-(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-([\w\d]+)\.plot$", plot_path)
            if matches is not None:
                groups = matches.groups()
                source_disk = groups[0]
                plot_k = int(groups[1])
                year = int(groups[2])
                month = int(groups[3])
                day = int(groups[4])
                hour = int(groups[5])
                minute = int(groups[6])
                plot_id = groups[7]
                plot_timestamp = datetime.timestamp(datetime(year, month, day, hour, minute))
                job = cls(
                    plot_id=plot_id,
                    plot_k=plot_k,
                    plot_timestamp=plot_timestamp,
                    source_disk=source_disk,
                    dest_disk=dest_disk,
                    start_timestamp=start_timestamp,
                    bw_limit=bw_limit
                )
                jobs.append(job)
        return jobs

    def __init__(
        self,
        plot_id: str,
        plot_k: int,
        plot_timestamp: float,
        source_disk: int,
        dest_disk: int,
        start_timestamp: float,
        bw_limit: int
    ) -> None:
        self.plot_id = plot_id
        self.plot_k = plot_k
        self.plot_timestamp = plot_timestamp
        self.source_disk = source_disk
        self.dest_disk = dest_disk
        self.start_timestamp = start_timestamp
        self.bw_limit = bw_limit
        self.timestamp = datetime.timestamp(datetime.now())

    def progress(self) -> float:
        now = datetime.timestamp(datetime.now())
        elapsed = now - self.start_timestamp
        return min(((elapsed * self.bw_limit * 1000) * 0.8) / plot_util.get_plotsize(self.plot_k), 1)


class IngressArchiveJob:
    job_id: str
    plot_id: str
    plot_k: int
    plot_timestamp: float
    disk: int
    is_local: bool
    transferred_bytes: int
    prev_transferred_bytes: Optional[List[Tuple[int, int]]]
    timestamp: float

    @classmethod
    def get_archive_running_jobs(cls, arch_cfg: configuration.Archiving, prev_jobs: List["IngressArchiveJob"]) -> List["IngressArchiveJob"]:
        local_jobs = archive.get_running_archive_jobs(arch_cfg)

        target = arch_cfg.target_definition()
        variables = {**os.environ, **arch_cfg.environment()}
        dest = target.transfer_process_argument_prefix.format(**variables)

        path = dest.strip() if dest.strip().endswith('/') else dest.strip() + '/'
        jobs = []
        timeout = 40
        try:
            completed_process = subprocess.run(
                ['find', path, '-name', '.plot-k*', '-ls'],
                env={**os.environ},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as e:
            print(e)
        else:
            stdout = completed_process.stdout.decode('utf-8', errors='ignore').strip()
            stderr = completed_process.stderr.decode('utf-8', errors='ignore').strip()
            for line in stderr.splitlines():
                raise Exception(line)
            for line in stdout.splitlines():
                line = line.strip()
                split = line.split()
                if len(split) != 11:
                    continue
                job = split[10]
                space = split[6]
                job = job.strip()
                transferred_bytes = int(space)
                matches = re.search(rf"^{path}(\d{{3}})/\.plot-k(\d{{2}})-(\d{{4}})-(\d{{2}})-(\d{{2}})-(\d{{2}})-(\d{{2}})-([\w\d]+)\.plot\.([\w\d]+)$", job)
                if matches is not None:
                    groups = matches.groups()
                    disk = int(groups[0])
                    plot_k = int(groups[1])
                    year = int(groups[2])
                    month = int(groups[3])
                    day = int(groups[4])
                    hour = int(groups[5])
                    minute = int(groups[6])
                    plot_id = groups[7]
                    job_id = groups[8]
                    is_local = any(plot_id in ' '.join(local_job.cmdline()) for local_job in local_jobs)
                    plot_timestamp = datetime.timestamp(datetime(year, month, day, hour, minute))
                    prev_transferred_bytes = []
                    if prev_jobs is not None:
                        prev_transferred_bytes = next(([(job.timestamp, job.transferred_bytes)] + job.prev_transferred_bytes for job in prev_jobs if job.job_id == job_id), [])
                    job = cls(
                        job_id=job_id,
                        plot_id=plot_id,
                        plot_k=plot_k,
                        plot_timestamp=plot_timestamp,
                        disk=disk,
                        is_local=is_local,
                        transferred_bytes=transferred_bytes,
                        prev_transferred_bytes=prev_transferred_bytes,
                    )
                    jobs.append(job)
        return jobs

    def __init__(
        self,
        job_id: str,
        plot_id: str,
        plot_k: int,
        plot_timestamp: float,
        disk: int,
        is_local: bool,
        transferred_bytes: int,
        prev_transferred_bytes: List[Tuple[int, int]] = []
    ) -> None:
        self.job_id = job_id
        self.plot_id = plot_id
        self.plot_k = plot_k
        self.plot_timestamp = plot_timestamp
        self.disk = disk
        self.is_local = is_local
        self.transferred_bytes = transferred_bytes
        self.prev_transferred_bytes = prev_transferred_bytes
        self.timestamp = datetime.timestamp(datetime.now())

    def progress(self) -> float:
        return min(self.transferred_bytes / plot_util.get_plotsize(self.plot_k), 1)

    def estimated_remaining_time(self) -> Optional[int]:
        rate = self.estimated_transfer_rate()
        if rate is None or rate == 0:
            return None
        left = plot_util.get_plotsize(self.plot_k) - self.transferred_bytes
        return max(int(left / rate), 0)

    def estimated_transfer_rate(self) -> Optional[float]:
        if len(self.prev_transferred_bytes) == 0:
            return None
        prevs = sorted(self.prev_transferred_bytes, key=lambda tuple: tuple[0])
        prev = prevs[0]
        seconds = self.timestamp - prev[0]
        if seconds == 0:
            return None
        bytes_delta = self.transferred_bytes - prev[1]
        return bytes_delta / seconds
