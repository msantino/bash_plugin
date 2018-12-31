# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from airflow.hooks.base_hook import BaseHook
import os
import signal
from tempfile import NamedTemporaryFile, TemporaryDirectory, gettempdir
from builtins import bytes
from subprocess import Popen, STDOUT, PIPE
from airflow.exceptions import AirflowException

logging = logging.getLogger(__name__)


class BashHook(BaseHook):
    """
    Interact to Operational System to execute bash commands

    :param bash_command: Command to execute on bash
    :type bash_command: str
    :param task_id: Task instance from DAG
    :type task_id: str
    :return: None
    """

    sp = None

    def __init__(self,
                 bash_command,
                 task_id,
                 *args, **kwargs):
        self.bash_command = bash_command
        self.task_id = task_id
        self.output_encoding = 'utf-8'
        self.env = None

    def execute(self):

        logging.info("Tmp dir root location: {}".format(gettempdir()))
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(self.bash_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                logging.info("Temporary script location: {}".format(script_location))

                def pre_exec():
                    # Restore default signal disposition and invoke setsid
                    for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                        if hasattr(signal, sig):
                            signal.signal(getattr(signal, sig), signal.SIG_DFL)
                    os.setsid()

                logging.info("Running command: %s", self.bash_command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env,
                    preexec_fn=pre_exec)

                self.sp = sp

                logging.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).strip()
                    logging.info(line)
                sp.wait()
                logging.info("Command exited with return code {}".format(sp.returncode))

                if sp.returncode:
                    raise AirflowException("Bash command failed")

                return line
