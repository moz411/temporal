import asyncio
from temporalio import activity
from ansible.executor.task_executor import TaskExecutor
from ansible.playbook.task import Task
from ansible.parsing.dataloader import DataLoader
from ansible.inventory.manager import InventoryManager
from ansible.vars.manager import VariableManager
from ansible.playbook.play_context import PlayContext
from ansible.inventory.host import Host
from ansible.plugins.connection.local import Connection

def parse_task_dict(task_dict: dict) -> Task:
    loader = DataLoader()
    inventory = InventoryManager(loader=loader, sources=["localhost,"])
    variable_manager = VariableManager(loader=loader, inventory=inventory)

    task_obj = Task.load(task_dict, variable_manager=variable_manager, loader=loader)
    return task_obj

@activity.defn
async def run_ansible_task(params) -> list:
    result = []
    host = Host(params.get('host', 'localhost'))
    task = parse_task_dict(params.get('task', {}))
    task_vars = {}
    play_context =  PlayContext()
    new_stdin =  {}
    loader =  DataLoader()
    shared_loader_obj =  loader
    final_q =  {}

    # res = AdHocCLI(args=cmd).run()
    executor_result = TaskExecutor(
            host,
            task,
            task_vars,
            play_context,
            new_stdin,
            loader,
            shared_loader_obj,
            final_q
        ).run()
    return executor_result
