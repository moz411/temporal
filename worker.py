import asyncio
import yaml
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import activity
from activities import AnsibleActivityManager
from workflow import PlaybookWorkflow, PlayWorkflow, HostWorkflow
from concurrent.futures import ThreadPoolExecutor

async def main():
    # Start Temporal client
    client = await Client.connect("temporal-frontend.temporal.svc:7233")

    # Start Ansible Activity Manager
    aam = AnsibleActivityManager("inventory.yml")

    # Run a worker for the workflow with dynamically created activities
    with ThreadPoolExecutor(max_workers=42) as executor:
        worker = Worker(
            client,
            task_queue="ansible-tasks",
            workflows=[PlaybookWorkflow, PlayWorkflow, HostWorkflow],
            activities=[aam.create_play, aam.run_ansible_task],
            activity_executor=executor,
        )
        print("Starting worker...")
        await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
