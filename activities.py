from ansible.executor.task_executor import TaskExecutor
from ansible.playbook.play_context import PlayContext
from ansible.parsing.dataloader import DataLoader
from ansible.inventory.manager import InventoryManager
from ansible.vars.manager import VariableManager
from ansible.playbook.task import Task
from ansible.playbook.play import Play
from ansible.plugins import loader as shared_loader_obj
from temporalio import activity

class AnsibleActivityManager:
    def __init__(self, inventory_path: str):
        self.loader = DataLoader()
        self.inventory = InventoryManager(loader=self.loader, sources=[inventory_path])
        self.variable_manager = VariableManager(loader=self.loader, inventory=self.inventory)
        self.play = None
        self.new_stdin = {}
        self.shared_loader_obj = shared_loader_obj
        self.final_q = {}
        self._compiled_blocks = {}

    @activity.defn(name="create_play")
    def create_play(self, play_dict: dict):
        self.play = Play().load(play_dict, variable_manager=self.variable_manager, loader=self.loader)
        self.hosts = self.inventory.list_hosts(play_dict["hosts"])
        self.block = self.play.compile()[0]
        self.block._play = self.play
        self._compiled_blocks[play_dict["name"]] = (self.play, self.block)
        return [host.name for host in self.hosts]

    @activity.defn(name="run_ansible_task")
    def run_ansible_task(self, params: dict) -> dict:
        activity_host = self.inventory.get_host(params['host'])
        ctx = self.build_play_context(activity_host)
        task = Task.load(params["task"], variable_manager=self.variable_manager, loader=self.loader)
        task_vars = self.variable_manager.get_vars(play=self.play, host=activity_host)
        task._parent = self.block

        result = TaskExecutor(
            activity_host,
            task,
            task_vars,
            ctx,
            self.new_stdin,
            self.loader,
            self.shared_loader_obj,
            self.final_q
        ).run()

        if result.get("unreachable") or result.get("failed"):
            raise Exception(f"Task failed: {result}")
        return result


    def build_play_context(self, host):
        # Récupérer toutes les variables pour ce host
        host_vars = self.variable_manager.get_vars(host=host)

        # Créer un PlayContext vide
        ctx = PlayContext()
        
        # Injecter les variables SSH
        ctx.remote_user     = host_vars.get("ansible_user")
        ctx.port            = host_vars.get("ansible_port")
        ctx.connection      = host_vars.get("ansible_connection", "ssh")
        ctx.private_key_file = host_vars.get("ansible_ssh_private_key_file")
        ctx.password        = host_vars.get("ansible_ssh_pass")
        ctx.ssh_common_args = host_vars.get("ansible_ssh_common_args")
        ctx.ssh_extra_args  = host_vars.get("ansible_ssh_extra_args")
        ctx.sftp_extra_args = host_vars.get("ansible_sftp_extra_args")
        ctx.scp_extra_args  = host_vars.get("ansible_scp_extra_args")
        ctx.become          = host_vars.get("ansible_become", False)
        ctx.become_method   = host_vars.get("ansible_become_method", "sudo")
        ctx.become_user     = host_vars.get("ansible_become_user")
        ctx.become_pass     = host_vars.get("ansible_become_pass")

        return ctx


    def create_activity_per_host(self, task_dict: dict):
        activities = []

        for host in self.hosts:
            task_name = f"{host.name}: {task_dict['name']}"

            @activity.defn(name=task_name)
            async def _activity(params) -> dict:
                activity_host = self.inventory.get_host(params['host'])
                task = Task.load(params["task"], variable_manager=self.variable_manager, loader=self.loader)
                task_vars = self.variable_manager.get_vars(play=self.play, host=host)
                task._parent = self.block

                result = TaskExecutor(
                    activity_host,
                    task,
                    task_vars,
                    self.play_context,
                    self.new_stdin,
                    self.loader,
                    self.shared_loader_obj,
                    self.final_q
                ).run()

                if result.get("unreachable") or result.get("failed"):
                    raise Exception(f"Task failed: {result}")
                return result

            activities.append(_activity)

        return activities

    def list_activities(self, play_dict: dict):
        activities = []
        for task in play_dict["tasks"]:
            for host in self.hosts:
                activities.append(f"{host.name}: {task['name']}")
        return activities