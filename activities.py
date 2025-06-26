import subprocess
import json
from temporalio import activity

@activity.defn
async def run_ansible_task(task_data: dict, host: str) -> dict:
    task_name = task_data.get("name", "ad-hoc-task")
    module = task_data.get("action", {}).get("module", "debug")
    args = task_data.get("action", {}).get("args", {})

    # Format args pour CLI : {'msg': 'hello'} → "msg=hello"
    args_cli = " ".join(f"{k}={json.dumps(v) if isinstance(v, (dict, list)) else v}" for k, v in args.items())

    # Commande ansible
    cmd = [
        "ansible",
        host,
        "-m",
        module,
        "-a",
        args_cli,
        "--connection=local",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return {
            "task": task_name,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "rc": result.returncode,
        }
    except subprocess.CalledProcessError as e:
        return {
            "task": task_name,
            "stdout": e.stdout,
            "stderr": e.stderr,
            "rc": e.returncode,
            "failed": True,
        }
