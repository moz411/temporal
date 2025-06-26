import asyncio
from temporalio.client import Client
from temporalio.worker import Worker
from activities import run_ansible_task
from workflow import AnsiblePlaybookWorkflow

async def main():
    # Start client
    client = await Client.connect("temporal-frontend.temporal.svc:7233")

    # Run a worker for the workflow
    worker = Worker(client, task_queue="ansible-tasks", 
                            workflows=[AnsiblePlaybookWorkflow],
                            activities=[run_ansible_task])
    print("Starting worker...")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
