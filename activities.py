import asyncio
from temporalio import activity
from ansible.playbook.play import Play
from ansible.playbook.block import Block
from ansible.playbook.task import Task
from ansible.executor.task_executor import TaskExecutor
from ansible.parsing.dataloader import DataLoader
from ansible.inventory.manager import InventoryManager
from ansible.vars.manager import VariableManager
from ansible.executor.task_queue_manager import TaskQueueManager
from ansible.playbook.play_context import PlayContext
from ansible.inventory.host import Host
from ansible.plugins import loader as first_loader

play_context = PlayContext()
play_context.connection = "local"
loader = DataLoader()
inventory = InventoryManager(loader=loader, sources=["localhost ansible_connection=local ansible_python_intepreter='/usr/bin/python3,"])
variable_manager = VariableManager(loader=loader, inventory=inventory)

new_stdin =  {}
shared_loader_obj =  first_loader
final_q =  {}
play = Play().load(
        {
            "name": "Temporal Play",
            "hosts": "localhost",
            "gather_facts": True,
        },
        variable_manager=variable_manager,
        loader=loader,
    )
block = play.compile()[0]
block._play = play

tqm = None
try:
    tqm = TaskQueueManager(
        inventory=inventory,
        variable_manager=variable_manager,
        loader=loader,
        passwords={},
        stdout_callback="default",
    )
    tqm.run(play)
finally:
    if tqm:
        tqm.cleanup()
    loader.cleanup_all_tmp_files()

@activity.defn
async def run_ansible_task(params) -> list:
    host = Host(params["host"])
    params.pop("host")
    result = []
    task = Task.load(params, variable_manager=variable_manager, loader=loader)
    task_vars = variable_manager.get_vars(play=play, host=host)
    task._parent = block
    
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
