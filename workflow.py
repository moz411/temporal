import asyncio
import yaml
from temporalio.common import RetryPolicy
from temporalio.client import Client
from temporalio import workflow
from datetime import timedelta
from collections import defaultdict

@workflow.defn
class HostWorkflow:
    @workflow.run
    async def run(self, params) -> list:
        results = []
        host = params["host"]
        for task in params["tasks"]:
            result = await workflow.execute_activity(
                f"{host}: {task['name']}",
                task,
                schedule_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(
                    backoff_coefficient=2.0,
                    maximum_attempts=1,
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=2),
                    # non_retryable_error_types=["ValueError"],
                )
            )
            results.append(result)
        return results

@workflow.defn
class PlayWorkflow:
    @workflow.run
    async def run(self, play) -> list:
        results: dict[str, str] = {}
        activities = []
        futures = []
        with workflow.unsafe.sandbox_unrestricted():
            with open(f"{play['name']}_activities.txt") as f:
                activities = [line.strip() for line in f]
        tasks_per_host = defaultdict(list)
        for line in activities:
            host, _ = map(str.strip, line.split(":", 1))
            tasks_per_host[host] = play["tasks"]
        for host, tasks in tasks_per_host.items():
            id = f"{workflow.info().workflow_id}:  {host}"
            futures.append(workflow.execute_child_workflow(
                        HostWorkflow.run,
                        {'host': host, 'tasks': tasks},
                        id=id
                    ))
            # results[id] = res
        results = await asyncio.gather(*futures)
        return results

@workflow.defn
class PlaybookWorkflow:
    @workflow.run
    async def run(self, playbook: list[dict]) -> dict[str, list]:
        results: dict[str, str] = {}
        for play in playbook:
            name = play.get("name")
            id = f"{workflow.info().workflow_id}: {name}"
            res = await workflow.execute_child_workflow(
                    PlayWorkflow.run,
                    play,
                    id=id
                )
            results[id] = res
        return results

async def main():
    client = await Client.connect("temporal-frontend.temporal.svc:7233")
    filename = "playbook.yml"
    with open(filename) as f:
        playbook = yaml.safe_load(f)
    
    result = await client.execute_workflow(
        PlaybookWorkflow.run,
        playbook,
        id=filename,
        task_queue="ansible-tasks",
    )

if __name__ == "__main__":
    asyncio.run(main())
