from airflow.plugins_manager import AirflowPlugin

from bash_plugin.hooks.bash_hook import BashHook


class BashPlugin(AirflowPlugin):
    name = "bash_plugin"
    hooks = [BashHook]
