# Try to import HTCondor and associated packages
try:
    import htcondor
    import classad
except ImportError:
    htcondor = None
    classad = None

from os.path import join
from collections import namedtuple
import os
import stat
import asyncio
import subprocess
import re

from snakemake.executors import ClusterExecutor, sleep
from snakemake.executors.common import join_cli_args, format_cli_arg
from snakemake.logging import logger
from snakemake.exceptions import WorkflowError
from snakemake.interfaces import (
    DAGExecutorInterface,
    ExecutorJobInterface,
    WorkflowExecutorInterface,
)
from snakemake.resources import DefaultResources
from snakemake.common import async_lock

HTCondorJob = namedtuple(
    "HTCondorJob", "job jobid htcondor_jobid htcondor_logfile callback error_callback"
    )

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

    def format_inputs(self, job):
        input_str = job.input

        # Split the string by spaces to create a list of file paths
        input_list = str(input_str).split()

        # Join the elements of the list using ', ' as the separator
        formatted_inputs = ', '.join(input_list)
        return formatted_inputs

    def get_job_args(self, job):
        return super().get_job_args(job)

    def format_condor_submit(self, job, htcondor_logfile):
        if str(self.cores) == "all":
            self.cores = 1
        jobscript = os.path.basename(self.get_jobscript(job))
        job_submit = htcondor.Submit({
            "executable": "{}".format(os.path.relpath(self.get_jobscript(job))),
            # "executable": "{}".format("sleep.sh"),
            "transfer_executable": "YES",
            # Needed to set the local cache on the EP
            "environment": "XDG_CACHE_HOME=$$(CondorScratchDir)",
            "log": htcondor_logfile,
            "output": self.get_jobname(job) + ".out",
            "error": self.get_jobname(job) + ".err",
            "request_memory": str(job.resources.get("mem_mb", 1024)) + "MB",
            "request_cpus": str(job.resources.get("_cores", 1)),
            # Additional HTCondor job options here...
            "request_disk": str(job.resources.get("disk_mb", 2048)) + "MB",
            "transfer_input_files": "{}, {}, {}, {}".format(self.get_jobscript(job),self.get_snakefile(), self.format_inputs(job), self.workflow.overwrite_configfiles[0]),
            "should_transfer_files": "YES",
            "when_to_transfer_output": "ON_EXIT_OR_EVICT",
            "universe": "container",
            "container_image": "docker://snakemake/snakemake",
            "preserve_relative_paths": "true",     
        })
        return job_submit

    def condor_submit_job(self, job, htcondor_logfile):

        local_provider_name = htcondor.param.get('LOCAL_CREDMON_PROVIDER_NAME')
        if local_provider_name is None:
            print('Local provider not named, aborting.')
            exit(1)
        magic = f"LOCAL:{local_provider_name}"
        binary_magic = bytes(magic, 'utf-8')
        credd = htcondor.Credd()
        credd.add_user_cred(htcondor.CredTypes.Kerberos, binary_magic)

        submit_file = self.format_condor_submit(job, htcondor_logfile)
        schedd = htcondor.Schedd()
        submit_result = schedd.submit(submit_file)

        return submit_result.cluster()

    def format_job_exec(self, job):
        # Generate the command to execute Snakemake for each job.
        prefix = self.get_job_exec_prefix(job)
        if prefix:
            prefix += " &&"
        suffix = self.get_job_exec_suffix(job)
        if suffix:
            suffix = f"&& {suffix}"

        # Need a way to replace the configfile to run on the EP.
        new_config_file = os.path.basename(self.workflow.overwrite_configfiles[0])
        pattern = r"(--configfiles\s+)\'[^\']*\'"
        replacement = fr"\1'{new_config_file}'"
        modified_general_args = re.sub(pattern, replacement, self.general_args)

        return join_cli_args(
            [
                prefix,
                self.get_envvar_declarations(),
                self.get_python_executable(),
                "-m snakemake",
                format_cli_arg("--snakefile", self.snakefile),
                self.get_job_args(job),
                modified_general_args,
                suffix,
            ]
        )

        # return (
        #     f"{self.get_python_executable()} -m snakemake --snakefile {self.snakefile} "
        #     f"--cores {job.resources.get('_cores', 1)} " #{self.get_default_remote_provider_args()}
        #     f"{self.get_default_resources_args()} {self.get_job_args(job)} "
        #     f"{self.get_workdir_arg()} --nolock {job.rule.name}"
        # )

    def _set_job_resources(self, job: ExecutorJobInterface):
        """
        Given a particular job, generate the resources that it needs,
        including default regions and the virtual machine configuration
        """
        self.default_resources = DefaultResources(
            from_other=self.workflow.default_resources
        )

    def status(self, condor_job):
        jobid = condor_job.jobid
        htcondor_logfile = f".snakemake/htcondor_logs/{jobid}.log"
        failed_states = [
            htcondor.JobEventType.JOB_HELD,
            htcondor.JobEventType.JOB_ABORTED,
            htcondor.JobEventType.EXECUTABLE_ERROR,
        ]
        status_report = ""
        try:
            jel = htcondor.JobEventLog(htcondor_logfile)
            for event in jel.events(stop_after=0):
                if event.type in failed_states:
                    status_report = f"Rule {condor_job.job} encountered a failure"
                    print(status_report)
                    return "failed"
                elif event.type is htcondor.JobEventType.JOB_TERMINATED:
                    if event["ReturnValue"] == 0:
                        status_report = f"Rule {condor_job.job} completed successfully"
                        print(status_report)
                        return "success"
                    status_report = f"Rule {condor_job.job} was terminated"
                    return "failed"
                else:
                    if event.type is htcondor.JobEventType.EXECUTE:
                        status_report = f"Rule {condor_job.job} is running through HTCondor with id {condor_job.htcondor_jobid}"
                        continue
                    if event.type is htcondor.JobEventType.SUBMIT:
                        status_report = f"Rule {condor_job.job} was submitted to HTCondor with id {condor_job.htcondor_jobid} and is idle"
                        continue
                    
        except OSError as e:
            print("failed: {}".format(e))
            return "failed"
        print(status_report)
        return "running"



    async def _wait_for_jobs(self):

        while True:
            # always use self.lock to avoid race conditions
            async with async_lock(self.lock):
                if not self.wait:
                    return
                active_jobs = self.active_jobs
                self.active_jobs = list()
                still_running = list()

            # Loop through active jobs and act on status
            for j in active_jobs:
                # use self.status_rate_limiter to avoid too many API calls.
                async with self.status_rate_limiter:
                    try:
                        status = self.status(j)

                        # The operation is done
                        if status == "failed":
                            j.error_callback(j.job)
                        elif status == "success":
                                j.callback(j.job)
                        
                        # The operation is still running
                        else:
                            still_running.append(j)
                    except RuntimeError:
                        # job did not complete
                        j.error_callback(j.job)

            async with async_lock(self.lock):
                self.active_jobs.extend(still_running)
            await sleep()





    def cancel(self):
        # super().cancel()
        for job_info in self.active_jobs:
            job_id = job_info.htcondor_jobid
            subprocess.run(["condor_rm", str(job_id)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.shutdown()

    def get_jobname(self, job):
        # Override this method to generate a unique job name for HTCondor.
        # You may use job.format_wildcards to create a custom name.
        return super().get_jobname(job)

    def get_jobscript(self, job):
        f = self.get_jobname(job)

        if os.path.sep in f:
            raise WorkflowError(
                "Path separator ({}) found in job name {}. "
                "This is not supported.".format(os.path.sep, f)
            )

        return os.path.join(self.tmpdir, f)

    def write_jobscript(self, job, jobscript):
        exec_job = self.format_job_exec(job)

        try:
            content = self.jobscript.format(
                properties=job.properties(cluster=self.cluster_params(job)),
                exec_job=exec_job,
            )
        except KeyError as e:
            if self.is_default_jobscript:
                raise e
            else:
                raise WorkflowError(
                    f"Error formatting custom jobscript {self.workflow.jobscript}: value for {e} not found.\n"
                    "Make sure that your custom jobscript is defined as expected."
                )

        logger.debug(f"Jobscript:\n{content}")
        with open(jobscript, "a") as f:
            print(content, file=f)
        os.chmod(jobscript, os.stat(jobscript).st_mode | stat.S_IXUSR | stat.S_IRUSR)

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
        jobid = job.jobid
        htcondor_logfile = f".snakemake/htcondor_logs/{jobid}.log"
        os.makedirs(os.path.dirname(htcondor_logfile), exist_ok=True)

        jobscript = self.get_jobscript(job)
        
        self.write_jobscript(job, jobscript)
        jobscript =  os.path.basename(jobscript)

        htcondor_jobid = self.condor_submit_job(job, htcondor_logfile)

        self.active_jobs.append(
            HTCondorJob(job, jobid, htcondor_jobid, htcondor_logfile, callback, error_callback)
        )
