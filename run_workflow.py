import asyncio
from datetime import timedelta

from temporalio.client import Client
from temporalio.common import RetryPolicy

from activities import TASK_QUEUE_NAME
from workflows import LeadsquaredWorkflow


async def main():
    client = await Client.connect("localhost:7233")

    await client.execute_workflow(
        LeadsquaredWorkflow.run,
        id="leadsquared-workflow",
        task_queue=TASK_QUEUE_NAME,
        retry_policy=RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=10),
            maximum_attempts=3,
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
