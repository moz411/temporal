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
                "run_ansible_task",
                {"host": host, "task": task},
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
        futures = []
        hosts = await workflow.execute_activity(
            "create_play",
            play,
            schedule_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                backoff_coefficient=2.0,
                maximum_attempts=1,
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=2),
                # non_retryable_error_types=["ValueError"],
            )
        )
        for host in hosts:
            fut = workflow.start_child_workflow(
                    HostWorkflow.run,
                    {"host": host, "tasks": play["tasks"]},
                )
            futures.append(fut)
        result = await asyncio.gather(*futures)
        return result

@workflow.defn
class PlaybookWorkflow:
    @workflow.run
    async def run(self, playbook: list[dict]) -> list:
        results = []
        activities = []
        futures = []
        for play in playbook:
            name = play.get("name")
            id = f"{workflow.info().workflow_id}: {name}"
            result = await workflow.execute_child_workflow(
                    PlayWorkflow.run,
                    play,
                    id=id
                )
            results.append(result)
        # for play, fut in futures.items():
            # try:
                # results[play] = await fut
            # except Exception as e:
            #     results[play] = {"status": "failed", "error": str(e)}
        # results = await asyncio.gather(*futures)
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
