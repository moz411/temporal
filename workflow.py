import asyncio
import yaml
from temporalio.client import Client
from temporalio import workflow
from datetime import timedelta
from activities import run_ansible_task

@workflow.defn
class AnsiblePlaybookWorkflow:
    @workflow.run
    async def run(self, playbook: list[dict]) -> list[dict]:
        results = []
        for play in playbook:
            for task in play.get("tasks", []):
                print(task)
                result = await workflow.execute_activity(
                    run_ansible_task,
                    task,
                    schedule_to_close_timeout=timedelta(minutes=2),
                )
                results.append(result)
        return results

async def main():
    client = await Client.connect("temporal-frontend.temporal.svc:7233")
    with open("site.yml") as f:
        playbook = yaml.safe_load(f)

    result = await client.execute_workflow(
        AnsiblePlaybookWorkflow.run,
        playbook,
        id="ansible-playbook-wf",
        task_queue="ansible-tasks",
    )
    print("Workflow result:", result)

if __name__ == "__main__":
    asyncio.run(main())