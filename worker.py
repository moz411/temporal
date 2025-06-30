import asyncio
import yaml
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import activity
from activities import run_ansible_task
from workflow import PlaybookWorkflow, PlayWorkflow

def _create_dynamic_activity(task):
    """Create an activity for the given Ansible task."""
    @activity.defn(name=task["name"])
    async def _activity(params) -> dict:
        return await run_ansible_task(params)

    return _activity

async def main():
    # Start client
    client = await Client.connect("temporal-frontend.temporal.svc:7233")

    # Load playbook to dynamically create activities for each task
    with open("playbook.yml") as f:
        playbook = yaml.safe_load(f)

    activities = [run_ansible_task]
    for play in playbook:
        for task in play["tasks"]:
            activities.append(_create_dynamic_activity(task))

    # Run a worker for the workflow with dynamically created activities
    worker = Worker(
        client,
        task_queue="ansible-tasks",
        workflows=[PlaybookWorkflow, PlayWorkflow],
        activities=activities,
    )
    print("Starting worker...")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
