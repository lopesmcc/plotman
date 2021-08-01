import os
import re
import subprocess
from datetime import datetime
from typing import Tuple, List, Optional


class ArchiveJob:
    job_id: str
    plot_id: str
    plot_k: int
    plot_timestamp: float
    disk: int
    transferred_bytes: int
    prev_transferred_bytes: Optional[List[Tuple[int, int]]]
    timestamp: float

    @classmethod
    def get_running_jobs(cls, farm_path: str, prev_jobs: List["ArchiveJob"] = []) -> List["ArchiveJob"]:
        path = farm_path.strip() if farm_path.strip().endswith('/') else farm_path.strip() + '/'
        jobs = []
        timeout = 20
        try:
            completed_process = subprocess.run(
                ['find', path, '-name', '.plot-k*', '-ls', '|', 'awk', '-v', "OFS=':'", '{print $7, $11}'],
                env={**os.environ},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as e:
            print(e)
        else:
            stdout = completed_process.stdout.decode('utf-8', errors='ignore').strip()
            for line in stdout.splitlines():
                line = line.strip()
                split = line.split(':')
                if len(split) != 2:
                    continue
                space, job = split
                job = job.strip()
                transferred_bytes = int(space)
                matches = re.search(rf"^{path}(\d{3})/\.plot-k(\d{2})-(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-([\w\d]+)\.plot\.(\S+)$", job)
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
                    plot_timestamp = datetime.timestamp(datetime(year, month, day, hour, minute))
                    prev_transferred_bytes = []
                    if prev_jobs is not None:
                        prev_transferred_bytes = next(([(job.timestamp, job.transferred_bytes)] + job.prev_transferred_bytes for job in prev_jobs if job.job_id == job_id), default=[])
                    job = cls(
                        job_id=job_id,
                        plot_id=plot_id,
                        plot_k=plot_k,
                        plot_timestamp=plot_timestamp,
                        disk=disk,
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
        transferred_bytes: int,
        prev_transferred_bytes: List[Tuple[int, int]] = []
    ) -> None:
        self.job_id = job_id
        self.plot_id = plot_id
        self.plot_k = plot_k
        self.plot_timestamp = plot_timestamp
        self.disk = disk
        self.transferred_bytes = transferred_bytes
        self.prev_transferred_bytes = prev_transferred_bytes
        self.timestamp = datetime.timestamp(datetime.now())

    def progress(self) -> float:
        return 1

    def estimated_time_left(self) -> int:
        return 1
