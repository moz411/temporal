import asyncio
from temporalio import activity
from ansible.playbook.play import Play
from ansible.playbook.block import Block
from ansible.playbook.task import Task
from ansible.executor.task_executor import TaskExecutor
from ansible.parsing.dataloader import DataLoader
from ansible.inventory.manager import InventoryManager
from ansible.vars.manager import VariableManager
from ansible.playbook.play_context import PlayContext
from ansible.inventory.host import Host
from ansible.plugins import loader as first_loader


@activity.defn
async def run_ansible_task(params) -> list:
    result = []
    play_context = PlayContext()
    play_context.connection = "local"
    host = Host(params.get('host', 'localhost'))
    task_dict = params.get('task', {})
    loader = DataLoader()
    inventory = InventoryManager(loader=loader, sources=["localhost ansible_connection=local ansible_python_intepreter='/usr/bin/python3,"])
    variable_manager = VariableManager(loader=loader, inventory=inventory)
    task = Task.load(task_dict, variable_manager=variable_manager, loader=loader)
    task_vars = {}
    new_stdin =  {}
    shared_loader_obj =  first_loader
    final_q =  {}

    play = Play().load(
        {
            "name": "Temporal Play",
            "hosts": "localhost",
            "gather_facts": False,
            "tasks": [task_dict],  # nécessaire pour initialiser correctement les attrs internes
        },
        variable_manager=variable_manager,
        loader=loader,
    )

    block = play.compile()[0]  # → premier bloc (Block) compilé
    # Rattacher proprement les références
    task._parent = block
    block._play = play  # ← ceci évite ton erreur
    
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
