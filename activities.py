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
        self.play_context = PlayContext()
        self.play_context.connection = "local"
        self.new_stdin = {}
        self.shared_loader_obj = shared_loader_obj
        self.final_q = {}
        self._compiled_blocks = {}

    def create_play(self, play_dict: dict):
        self.play = Play().load(play_dict, variable_manager=self.variable_manager, loader=self.loader)
        self.block = self.play.compile()[0]
        self.block._play = self.play
        self._compiled_blocks[play_dict["name"]] = (self.play, self.block)

    def create_activity_per_host(self, task_dict: dict):
        activities = []
        hosts = self.inventory.list_hosts(task_dict["hosts"])

        for host in hosts:
            task_name = f"{host.name}: {task_dict['name']}"

            @activity.defn(name=task_name)
            async def _activity(params) -> dict:
                task = Task.load(params, variable_manager=self.variable_manager, loader=self.loader)
                task_vars = self.variable_manager.get_vars(play=self.play, host=host)
                task._parent = self.block

                result = TaskExecutor(
                    host,
                    task,
                    task_vars,
                    self.play_context,
                    self.new_stdin,
                    self.loader,
                    self.shared_loader_obj,
                    self.final_q
                ).run()
                return result

            activities.append(_activity)

        return activities

    def list_activities(self, play_dict: dict):
        activities = []
        for task in play_dict["tasks"]:
            hosts = self.inventory.list_hosts(task["hosts"])
            for host in hosts:
                activities.append(f"{host.name}: {task['name']}")
        return activities