import asyncio
import sys

from temporalio.client import Client
from temporalio.worker import Worker

from activities import (
    TASK_QUEUE_NAME,
    activities,
    email_campaign_stats,
    lead_properties,
    load_activities_to_snowflake,
    load_email_campaign_stats_to_snowflake,
    load_lead_properties_to_snowflake,
    load_opportunities_to_snowflake,
    load_sales_activities_to_snowflake,
    load_tasks_to_snowflake,
    load_user_by_id_to_snowflake,
    load_users_to_snowflake,
    opportunities,
    sales_activities,
    tasks,
    total_email_campaign_stats_pages,
    total_lead_properties_pages,
    user_by_id,
    users,
)
from workflows import LeadsquaredWorkflow


async def main():
    client = await Client.connect("localhost:7233")

    try:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE_NAME,
            workflows=[LeadsquaredWorkflow],
            activities=[
                total_lead_properties_pages,
                lead_properties,
                load_lead_properties_to_snowflake,
                opportunities,
                load_opportunities_to_snowflake,
                activities,
                load_activities_to_snowflake,
                tasks,
                load_tasks_to_snowflake,
                sales_activities,
                load_sales_activities_to_snowflake,
                users,
                load_users_to_snowflake,
                user_by_id,
                load_user_by_id_to_snowflake,
                total_email_campaign_stats_pages,
                email_campaign_stats,
                load_email_campaign_stats_to_snowflake,
            ],
        )
        await worker.run()
    except asyncio.exceptions.CancelledError:
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
