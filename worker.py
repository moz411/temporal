import asyncio
import yaml
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import activity
from activities import create_activities, list_activities
from workflow import PlaybookWorkflow, PlayWorkflow, HostWorkflow
from concurrent.futures import ThreadPoolExecutor

async def main():
    # Start client
    client = await Client.connect("temporal-frontend.temporal.svc:7233")

    # Load playbook to dynamically create activities for each task
    with open("playbook.yml") as f:
        playbook = yaml.safe_load(f)

    activities = []
    for play in playbook:
        for task in play["tasks"]:
            task["hosts"] = play["hosts"]
            activities += create_activities(task)
        with open(f"{play['name']}_activities.txt", "w") as f:
            lines = list_activities(play)
            f.writelines(f"{line}\n" for line in lines)

    # Run a worker for the workflow with dynamically created activities
    with ThreadPoolExecutor(max_workers=4) as executor:
        worker = Worker(
            client,
            task_queue="ansible-tasks",
            workflows=[PlaybookWorkflow, PlayWorkflow, HostWorkflow],
            activities=activities,
            activity_executor=executor,
        )
        print("Starting worker...")
        await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
