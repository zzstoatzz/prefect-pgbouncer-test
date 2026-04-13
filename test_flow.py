"""Simple E2E test: run a flow against a Prefect server using PgBouncer."""

import asyncio
import httpx
from prefect import flow, task


@task
def add(a: int, b: int) -> int:
    return a + b


@flow
def math_flow(x: int = 1, y: int = 2) -> int:
    result = add(x, y)
    print(f"{x} + {y} = {result}")
    return result


async def check_server_health(api_url: str, retries: int = 30, delay: float = 2.0):
    """Wait for the server to be healthy."""
    async with httpx.AsyncClient() as client:
        for i in range(retries):
            try:
                resp = await client.get(f"{api_url}/health")
                if resp.status_code == 200:
                    print(f"server healthy after {i * delay:.0f}s")
                    return
            except httpx.ConnectError:
                pass
            await asyncio.sleep(delay)
    raise RuntimeError(f"server not healthy after {retries * delay:.0f}s")


async def verify_flow_runs(api_url: str):
    """Verify flow runs were recorded in the database."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{api_url}/flow_runs/filter",
            json={"limit": 10},
        )
        resp.raise_for_status()
        runs = resp.json()
        assert len(runs) > 0, "no flow runs found"
        for run in runs:
            assert run["state"]["type"] == "COMPLETED", (
                f"flow run {run['id']} state: {run['state']['type']}"
            )
        print(f"verified {len(runs)} completed flow run(s)")


async def main():
    api_url = "http://localhost:4200/api"

    print("waiting for server...")
    await check_server_health(api_url)

    print("running flows...")
    for i in range(3):
        math_flow(i, i + 1)

    print("verifying flow runs in database...")
    await verify_flow_runs(api_url)

    print("all good!")


if __name__ == "__main__":
    asyncio.run(main())
