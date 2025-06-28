import asyncio
from temporalio import activity
from ansible.playbook.task import Task

@activity.defn
async def run_ansible_task(params) -> dict:
    # host_list = ['localhost', 'www.example.com', 'www.google.com']
    # context.CLIARGS = ImmutableDict(connection='local', 
    #     forks=10, become=None, become_method=None, become_user=None, check=False, diff=False, verbosity=0)
    # sources = ','.join(host_list)
    # if len(host_list) == 1:
    #     sources += ','
    # loader = DataLoader()
    # inventory = InventoryManager(loader=loader, sources=sources)
    # variable_manager = VariableManager(loader=loader, inventory=inventory)

    # tqm = TaskQueueManager(
    #     inventory=inventory,
    #     variable_manager=variable_manager,
    #     loader=loader,
    #     passwords=dict(vault_pass='secret'))

    # play_source = dict(
    #     name="Ansible Play",
    #     hosts=host_list,
    #     gather_facts='no',
    #     tasks=[
    #         dict(action=dict(module='command', args=dict(cmd='/usr/bin/uptime'))),
    #     ]
    # )

    # play = Play().load(play_source, variable_manager=variable_manager, loader=loader)
    # tqm.run(play)
    print(params)

# asyncio.run(run_ansible_task(None))