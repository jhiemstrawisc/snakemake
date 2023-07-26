
# Try to import HTCondor and associated packages
# try:
#     import htcondor
#     import classad
# except ImportError:
#     htcondor = None
#     classad = None

import htcondor
import classad
from random import randrange

import re
import time
from collections import namedtuple
import os
#import tempfile
import stat
import asyncio
import subprocess
from snakemake.executors import ClusterExecutor #, Mode
from snakemake.logging import logger
#from snakemake.common import Mode

from snakemake.logging import logger
from snakemake.exceptions import WorkflowError
from snakemake.executors import ClusterExecutor
from snakemake.interfaces import (
    DAGExecutorInterface,
    ExecutorJobInterface,
    WorkflowExecutorInterface,
)
from snakemake.resources import DefaultResources
from snakemake.common import async_lock

HTCondorJob = namedtuple("HTCondorJob", "job jobid htcondor_logfile")

class HTCondorExecutor(ClusterExecutor):
    def __init__(
        self,
        workflow,
        dag,
        cores,
        jobname="snakejob.{name}.{jobid}.sh",
        printreason=False,
        quiet=False,
        printshellcmds=False,
        cluster_config=None,
        local_input=None,
        restart_times=None,
        assume_shared_fs=False,
        max_status_checks_per_second=1,
        disable_default_remote_provider_args=False,
        disable_default_resources_args=False,
        disable_envvar_declarations=False,
        keepincomplete=False,
    ):

        super().__init__(
            workflow,
            dag,
            cores,
            jobname=jobname,
            printreason=printreason,
            quiet=quiet,
            printshellcmds=printshellcmds,
            cluster_config=cluster_config,
            local_input=local_input,
            restart_times=restart_times,
            assume_shared_fs=assume_shared_fs,
            max_status_checks_per_second=max_status_checks_per_second,
            disable_default_remote_provider_args=disable_default_remote_provider_args,
            disable_default_resources_args=disable_default_resources_args,
            disable_envvar_declarations=disable_envvar_declarations,
            keepincomplete=keepincomplete,
        )


    def get_snakefile(self):
        assert os.path.exists(self.workflow.main_snakefile)
        return self.workflow.main_snakefile

    # def get_job_args(self, job):
    #     # Customize the job arguments as needed for HTCondor.
    #     htcondor_args = {
    #         "RequestMemory": "2GB",
    #         "RequestCpus": str(self.cores),
    #         # Additional HTCondor job options here...
    #         "RequestDisk": "2GB",
    #         "TransferInput": " ".join(job.input),
    #         "ShouldTransferFiles": "YES",


    #     }
    #     return super().get_job_args(job) + self.format_htcondor_args(htcondor_args)

    # def format_htcondor_args(self, htcondor_args):
    #     # Format HTCondor arguments into a string that can be passed to the job script.
    #     return " ".join(
    #         f"{arg} = \"{val}\"" for arg, val in htcondor_args.items()
    #     )

    # def format_job_exec(self, job):
    #     # Generate the command to execute Snakemake for each job.
    #     return (
    #         f"{self.get_python_executable()} -m snakemake --snakefile {self.snakefile} "
    #         f"--cores {self.cores} {self.get_default_remote_provider_args()} "
    #         f"{self.get_default_resources_args()} {self.get_job_args(job)} "
    #         f"{self.get_workdir_arg()} --nolock {job.rule.name}"
    #     )

    async def _wait_for_jobs(self):
        time.sleep(2)

    def _set_job_resources(self, job: ExecutorJobInterface):
        """
        Given a particular job, generate the resources that it needs,
        including default regions and the virtual machine configuration
        """
        self.default_resources = DefaultResources(
            from_other=self.workflow.default_resources
        )

    # def status(self):
    #     # Use condor_q to get the status of HTCondor jobs.
    #     try:
    #         cmd = ["condor_q", "-format", "%d ", "ClusterId", "-format", "%d ", "ProcId", "-format", "%d\n", "JobStatus"]
    #         output = subprocess.check_output(cmd).decode().strip()
    #     except subprocess.CalledProcessError as e:
    #         logger.error(f"Error running condor_q: {e}")
    #         return

    #     job_statuses = {}
    #     lines = output.splitlines()
    #     for line in lines:
    #         cluster_id, proc_id, status = map(int, line.split())
    #         job_id = (cluster_id, proc_id)
    #         job_statuses[job_id] = status

    #     for job_info in self.active_jobs:
    #         job_id = job_info.jobid
    #         if job_id in job_statuses:
    #             status = job_statuses[job_id]
    #             if status == 4:  # HTCondor job completed successfully.
    #                 self.handle_job_success(job_info.job)
    #             elif status in [3, 5, 6, 7]:  # HTCondor job failed or was removed.
    #                 self.handle_job_error(job_info.job)

    # def cancel(self):
    #     super().cancel()
    #     # Cancel HTCondor jobs using condor_rm.
    #     for job_info in self.active_jobs:
    #         job_id = job_info.jobid
    #         subprocess.run(["condor_rm", str(job_id)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # def handle_job_success(self, job):
    #     super().handle_job_success(job)
    #     # You may want to perform additional cleanup here if needed.

    # def handle_job_error(self, job):
    #     super().handle_job_error(job)
    #     # You may want to perform additional cleanup here if needed.
    #     # For example, handle the case when HTCondor job fails or is removed.

    # def get_jobname(self, job):
    #     # Override this method to generate a unique job name for HTCondor.
    #     # You may use job.format_wildcards to create a custom name.
    #     return super().get_jobname(job)

    # def get_jobscript(self, job):
    #     f = self.get_jobname(job)

    #     if os.path.sep in f:
    #         raise WorkflowError(
    #             "Path separator ({}) found in job name {}. "
    #             "This is not supported.".format(os.path.sep, f)
    #         )

    #     return os.path.join(self.tmpdir, f)

    # def write_jobscript(self, job, jobscript):
    #     exec_job = self.format_job_exec(job)

    #     try:
    #         content = self.jobscript.format(
    #             properties=job.properties(cluster=self.cluster_params(job)),
    #             exec_job=exec_job,
    #         )
    #     except KeyError as e:
    #         if self.is_default_jobscript:
    #             raise e
    #         else:
    #             raise WorkflowError(
    #                 f"Error formatting custom jobscript {self.workflow.jobscript}: value for {e} not found.\n"
    #                 "Make sure that your custom jobscript is defined as expected."
    #             )

    #     logger.debug(f"Jobscript:\n{content}")
    #     with open(jobscript, "w") as f:
    #         print(content, file=f)
    #     os.chmod(jobscript, os.stat(jobscript).st_mode | stat.S_IXUSR | stat.S_IRUSR)

    def run(
            self,
            job: ExecutorJobInterface,
            callback=None,
            submit_callback=None,
            error_callback=None,
        ):
        """
        Submit a job to HTCondor.
        """
        super()._run(job)
        # jobid = job.jobid
        # htcondor_logfile = f".snakemake/htcondor_logs/%j.log"
        # os.makedirs(os.path.dirname(htcondor_logfile), exist_ok=True)

        # self.active_jobs.append(
        #     HTCondorJob(job, jobid, htcondor_logfile)
        # )
                
        command = self.format_job_exec(job)
        print("command:", command)

        # Use regex to find the part after --snakefile
        # Use regex to find the part after --snakefile
        match = re.search(r"--snakefile\s+'([^']+)'", command)

        if match:
            full_snakefile_path = match.group(1)
            # Extract the filename from the path
            filename = os.path.basename(full_snakefile_path)

            # Replace the full path with just the filename
            edited_input_string = re.sub(r"--snakefile\s+'([^']+)'", f"--snakefile '{filename}'", command)
            print("edited command:", edited_input_string)
        else:
            print("Snakefile path not found.")

        rand_file_suffix = randrange(10000)
        current_directory = os.getcwd()
        original_name = "temp-orig-snake-command-{}.sh".format(rand_file_suffix)
        new_name = "temp-new-snake-command-{}.sh".format(rand_file_suffix)

        with open(original_name, "w") as tmp_sf:
            tmp_sf.write(command)
                    
        with open(new_name, "w") as tmp_sf:
            edited_input_string = f'''#!/bin/bash
{edited_input_string}\n'''
            tmp_sf.write(edited_input_string)

        os.chmod(new_name, 0o755) 


        
        # Now start to build HTCondor submit file for the job
        job_submit = htcondor.Submit({
            "executable": "{}".format(new_name),
            "output": "foo.out",
            "error": "foo.err",
            "log": "foo.log",
            "request_cpus": 1, #We'll need to parse command eventually
            "request_memory": "128MB", #again...
            "request_disk": "1GB",
            "should_transfer_files": "yes",
            #"transfer_input_files": "temp-new-snake-command, {}".format(self.get_snakefile)
            "transfer_input_files": "{}".format(new_name),
            "universe": "container",
            "container_image": "docker://snakemake/snakemake",
        })

        schedd = htcondor.Schedd()
        submit_result = schedd.submit(job_submit)
        print(submit_result.cluster())



        print("snakefile: ", self.get_snakefile())













# from collections import namedtuple
# from io import StringIO
# from fractions import Fraction
# #import csv
# import os
# #import time
# #import shlex
# import subprocess
# #import uuid
# from snakemake.interfaces import (
#     DAGExecutorInterface,
#     ExecutorJobInterface,
#     WorkflowExecutorInterface,
# )

# from snakemake.logging import logger
# from snakemake.exceptions import WorkflowError
# from snakemake.executors import ClusterExecutor
# from snakemake.common import async_lock

# #HTCondorJob = namedtuple("HTCondorJob", "job jobid callback error_callback htcondor_logfile")

# #import tempfile
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
#         # from throttler import Throttler

#         # local_input = local_input or []
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

#         # self.status_rate_limiter = Throttler(
#         #     rate_limit=max_status_checks_per_second, period=1
#         # )

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






















#     def __init__(
#         self,
#         workflow: WorkflowExecutorInterface,
#         dag: DAGExecutorInterface,
#         cores,
#         jobname="snakejob.{name}.{jobid}",
#         printreason=False,
#         quiet=False,
#         printshellcmds=False,
#         restart_times=0,
#         max_status_checks_per_second=0.5,
#         cluster_config=None,
#         # Additional things to initialize, specific to HTCondor
#     ):
#         super().__init__(
#             workflow,
#             dag,
#             cores,
#             jobname=jobname,
#             printreason=printreason,
#             quiet=quiet,
#             printshellcmds=printshellcmds,
#             cluster_config=cluster_config,
#             restart_times=restart_times,
#             assume_shared_fs=False, # Do not assume a shared filesystem, always use CEDAR for file transfer
#             max_status_checks_per_second=max_status_checks_per_second,
#         )

#         # Additional things to initialize, specific to HTCondor

#     # Functions needed by every executor:

#     def shutdown(self):
#         """
#         Not yet sure if I need this
#         """
#         pass

#     def cancel(self):
#         """
#         cancel any jobs that haven't completed
#         """

#         # Flux executor example
#         # for job in self.active_jobs:
#         #     if not job.flux_future.done():
#         #         flux.job.cancel(self.f, job.jobid)
#         # self.shutdown()
#         pass

#     def _set_job_resources(self, job: ExecutorJobInterface):
#         """
#         Given a particular job, generate the resources that it needs,
#         including default regions and the virtual machine configuration
#         """
#         pass

#     def get_snakefile(self):
#         assert os.path.exists(self.workflow.main_snakefile)
#         return self.workflow.main_snakefile

#     # def _get_jobname(self, job: ExecutorJobInterface):
#     #     # Get the jobname

#     def run(
#         self,
#         job: ExecutorJobInterface,
#     ):
#         """
#         Submit a job to HTCondor.
#         """
#         super()._run(job)
#         self.active_jobs.append(
#             HTCondorJob(job, result["name"], jobid, callback, error_callback)
#         )
#     async def _wait_for_jobs(self): # This is an abstract method in the parent class
#         """
#         Wait for jobs to complete. This means requesting their status,
#         and then marking them as finished when a "done" parameter
#         shows up. Even for finished jobs, the status should still return
#         """
       



# import os
# import tempfile
# import stat
# import shutil
# import asyncio
# import subprocess
# from snakemake.executors import ClusterExecutor, Mode
# from snakemake.logging import logger

# class HTCondorExecutor(ClusterExecutor):
#     def __init__(self, workflow, dag, cores, cluster_config=None, **kwargs):
#         super().__init__(workflow, dag, cores, cluster_config=cluster_config, **kwargs)

#     def get_job_args(self, job):
#         # Customize the job arguments as needed for HTCondor.
#         htcondor_args = {
#             "RequestMemory": "2GB",
#             "RequestCpus": str(self.cores),
#             # Additional HTCondor job options here...
#         }
#         return super().get_job_args(job) + self.format_htcondor_args(htcondor_args)

#     def format_htcondor_args(self, htcondor_args):
#         # Format HTCondor arguments into a string that can be passed to the job script.
#         return " ".join(
#             f"+{arg} = \"{val}\"" for arg, val in htcondor_args.items()
#         )

#     def format_job_exec(self, job):
#         # Generate the command to execute Snakemake for each job.
#         return (
#             f"{self.get_python_executable()} -m snakemake --snakefile {self.snakefile} "
#             f"--cores {self.cores} {self.get_default_remote_provider_args()} "
#             f"{self.get_default_resources_args()} {self.get_job_args(job)} "
#             f"{self.get_workdir_arg()} --nolock {job.rule.name}"
#         )

#     def get_exec_mode(self):
#         return Mode.cluster

#     async def _wait_for_jobs(self):
#         while self.wait:
#             # Check the status of HTCondor jobs.
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

#     def shutdown(self):
#         super().shutdown()
#         # You may want to perform additional cleanup here if needed.

#     def get_jobname(self, job):
#         # Override this method to generate a unique job name for HTCondor.
#         # You may use job.format_wildcards to create a custom name.
#         return super().get_jobname(job)


