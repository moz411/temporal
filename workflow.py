import asyncio
import yaml
from temporalio.client import Client
from temporalio import workflow
from datetime import timedelta

@workflow.defn
class HostWorkflow:
    @workflow.run
    async def run(self, params) -> list[dict]:
        host: str = params.get("host", "")
        tasks: list[dict] = params.get("tasks", [])
        results = []
        for task in tasks:
            activity_name = task.get("name") or next(iter(task))
            params = {"host": host}
            result = await workflow.execute_activity(
                activity_name,
                params,
                schedule_to_close_timeout=timedelta(minutes=2),
            )
            results.append(result)
        return results

@workflow.defn
class AnsiblePlaybookWorkflow:
    @workflow.run
    async def run(self, playbook: list[dict]) -> dict[str, list[dict]]:
        results: dict[str, list[dict]] = {}
        for play in playbook:
            tasks = play.get("tasks", [])
            hosts = play.get("hosts", [])
            hosts_list = (
                [h.strip() for h in hosts.split(",")]
                if isinstance(hosts, str)
                else list(hosts)
            )
            for host in hosts_list:
                params = {"host": host, "tasks": tasks}
                res = await workflow.execute_child_workflow(
                    HostWorkflow.run,
                    params,
                    id=f"{workflow.info().workflow_id}-{host}",
                )
                results[host] = res
        return results

async def main():
    client = await Client.connect("temporal-frontend.temporal.svc:7233")
    # with open("playbook.yml") as f:
    #     playbook = yaml.safe_load(f)
    
    result = await client.execute_workflow(
        AnsiblePlaybookWorkflow.run,
        "playbook.yml",
        id="ansible-playbook-wf",
        task_queue="ansible-tasks",
    )
    # print("Workflow result:", result)
    # res = await AnsiblePlaybookWorkflow().run("playbook.yml")

if __name__ == "__main__":
    asyncio.run(main())
