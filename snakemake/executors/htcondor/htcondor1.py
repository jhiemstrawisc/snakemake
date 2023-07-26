# import os
# import tempfile
# import stat
# import asyncio
# import subprocess
# from snakemake.executors import ClusterExecutor, Mode
# from snakemake.logging import logger
# from snakemake.common import Mode



# class HTCondorExecutor(ClusterExecutor):
#     def __init__(
#         self,
#         workflow,
#         dag,
#         cores,
#         jobname="snakejob.{name}.{jobid}.sh",
#         printreason=False,
#         quiet=False,
#         printshellcmds=False,
#         cluster_config=None,
#         local_input=None,
#         restart_times=None,
#         assume_shared_fs=True,
#         max_status_checks_per_second=1,
#         disable_default_remote_provider_args=False,
#         disable_default_resources_args=False,
#         disable_envvar_declarations=False,
#         keepincomplete=False,
#     ):
#         from throttler import Throttler

#         local_input = local_input or []
#         super().__init__(
#             workflow,
#             dag,
#             cores,
#             jobname=jobname,
#             printreason=printreason,
#             quiet=quiet,
#             printshellcmds=printshellcmds,
#             cluster_config=cluster_config,
#             local_input=local_input,
#             restart_times=restart_times,
#             assume_shared_fs=assume_shared_fs,
#             max_status_checks_per_second=max_status_checks_per_second,
#             disable_default_remote_provider_args=disable_default_remote_provider_args,
#             disable_default_resources_args=disable_default_resources_args,
#             disable_envvar_declarations=disable_envvar_declarations,
#             keepincomplete=keepincomplete,
#         )

#         self.status_rate_limiter = Throttler(
#             rate_limit=max_status_checks_per_second, period=1
#         )

#     def get_job_args(self, job):
#         # Customize the job arguments as needed for HTCondor.
#         htcondor_args = {
#             "RequestMemory": "2GB",
#             "RequestCpus": str(self.cores),
#             # Additional HTCondor job options here...
#             "RequestDisk": "2GB",
#             "TransferInput": " ".join(job.input),
#             "ShouldTransferFiles": "YES",

#         }
#         return super().get_job_args(job) + self.format_htcondor_args(htcondor_args)

#     def format_htcondor_args(self, htcondor_args):
#         # Format HTCondor arguments into a string that can be passed to the job script.
#         return " ".join(
#             f"{arg} = \"{val}\"" for arg, val in htcondor_args.items()
#         )

#     def format_job_exec(self, job):
#         # Generate the command to execute Snakemake for each job.
#         return (
#             f"{self.get_python_executable()} -m snakemake --snakefile {self.snakefile} "
#             f"--cores {self.cores} {self.get_default_remote_provider_args()} "
#             f"{self.get_default_resources_args()} {self.get_job_args(job)} "
#             f"{self.get_workdir_arg()} --nolock {job.rule.name}"
#         )

#     async def _wait_for_jobs(self):
#         while self.wait:
#             self.status_rate_limiter.run()
#             self.status()
#             await asyncio.sleep(1)

#     def status(self):
#         # Use condor_q to get the status of HTCondor jobs.
#         try:
#             cmd = ["condor_q", "-format", "%d ", "ClusterId", "-format", "%d ", "ProcId", "-format", "%d\n", "JobStatus"]
#             output = subprocess.check_output(cmd).decode().strip()
#         except subprocess.CalledProcessError as e:
#             logger.error(f"Error running condor_q: {e}")
#             return

#         job_statuses = {}
#         lines = output.splitlines()
#         for line in lines:
#             cluster_id, proc_id, status = map(int, line.split())
#             job_id = (cluster_id, proc_id)
#             job_statuses[job_id] = status

#         for job_info in self.active_jobs:
#             job_id = job_info.jobid
#             if job_id in job_statuses:
#                 status = job_statuses[job_id]
#                 if status == 4:  # HTCondor job completed successfully.
#                     self.handle_job_success(job_info.job)
#                 elif status in [3, 5, 6, 7]:  # HTCondor job failed or was removed.
#                     self.handle_job_error(job_info.job)

#     def cancel(self):
#         super().cancel()
#         # Cancel HTCondor jobs using condor_rm.
#         for job_info in self.active_jobs:
#             job_id = job_info.jobid
#             subprocess.run(["condor_rm", str(job_id)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

#     def handle_job_success(self, job):
#         super().handle_job_success(job)
#         # You may want to perform additional cleanup here if needed.

#     def handle_job_error(self, job):
#         super().handle_job_error(job)
#         # You may want to perform additional cleanup here if needed.
#         # For example, handle the case when HTCondor job fails or is removed.

#     def get_jobname(self, job):
#         # Override this method to generate a unique job name for HTCondor.
#         # You may use job.format_wildcards to create a custom name.
#         return super().get_jobname(job)

#     def get_jobscript(self, job):
#         f = self.get_jobname(job)

#         if os.path.sep in f:
#             raise WorkflowError(
#                 "Path separator ({}) found in job name {}. "
#                 "This is not supported.".format(os.path.sep, f)
#             )

#         return os.path.join(self.tmpdir, f)

#     def write_jobscript(self, job, jobscript):
#         exec_job = self.format_job_exec(job)

#         try:
#             content = self.jobscript.format(
#                 properties=job.properties(cluster=self.cluster_params(job)),
#                 exec_job=exec_job,
#             )
#         except KeyError as e:
#             if self.is_default_jobscript:
#                 raise e
#             else:
#                 raise WorkflowError(
#                     f"Error formatting custom jobscript {self.workflow.jobscript}: value for {e} not found.\n"
#                     "Make sure that your custom jobscript is defined as expected."
#                 )

#         logger.debug(f"Jobscript:\n{content}")
#         with open(jobscript, "w") as f:
#             print(content, file=f)
#         os.chmod(jobscript, os.stat(jobscript).st_mode | stat.S_IXUSR | stat.S_IRUSR)

