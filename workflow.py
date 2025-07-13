import asyncio
import yaml
from temporalio.common import RetryPolicy
from temporalio.client import Client, WorkflowFailureError
from temporalio import workflow
from datetime import timedelta
from collections import defaultdict

@workflow.defn
class HostWorkflow:
    @workflow.run
    async def run(self, playbook) -> list:
        results = []
        for play in playbook:
            name = play.get("name")
            id = f"{workflow.info().workflow_id}: {name}"
            hosts = await workflow.execute_activity(
                "create_play",
                play,
                schedule_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(
                    backoff_coefficient=2.0,
                    maximum_attempts=1,
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=2),
                )
            )

            async def _execute_activity(host, tasks):
                activity_results = []
                for task in tasks:
                    try:
                        activity_result = await workflow.execute_activity(
                            "run_ansible_task",
                            {"host": host, "task": task},
                            schedule_to_close_timeout=timedelta(minutes=2),
                            retry_policy=RetryPolicy(
                                backoff_coefficient=2.0,
                                maximum_attempts=1,
                                initial_interval=timedelta(seconds=1),
                                maximum_interval=timedelta(seconds=2),
                            )
                        )
                        activity_results.append(activity_result)
                    except Exception as e:
                        activity_results.append(repr(e))
                        break
                return activity_results

            futures = [_execute_activity(host, play["tasks"]) for host in hosts]
            result = await asyncio.gather(*futures)
            results.append(result)
        return results

async def main():
    client = await Client.connect("temporal-frontend.temporal.svc:7233")
    filename = "playbook.yml"
    with open(filename) as f:
        playbook = yaml.safe_load(f)
    
    result = await client.execute_workflow(
        HostWorkflow.run,
        playbook,
        id=filename,
        task_queue="ansible-tasks",
    )

if __name__ == "__main__":
    asyncio.run(main())
