from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import (
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


@workflow.defn
class LeadsquaredWorkflow:
    @workflow.run
    async def run(self):
        page_number = 1

        pages = await workflow.execute_activity(
            total_lead_properties_pages,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(seconds=60),
                maximum_attempts=3,
            ),
        )

        if page_number >= 1:
            for page_number in range(1, pages):
                data, prospect_ids = await workflow.execute_activity(
                    lead_properties,
                    page_number,
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=1),
                        backoff_coefficient=2.0,
                        maximum_interval=timedelta(seconds=60),
                        maximum_attempts=5,
                    ),
                )

                if data:
                    await workflow.execute_activity(
                        load_lead_properties_to_snowflake,
                        data,
                        start_to_close_timeout=timedelta(seconds=60),
                    )

                if len(prospect_ids) > 0:
                    for prospect_id in prospect_ids:
                        data = await workflow.execute_activity(
                            opportunities,
                            prospect_id,
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(
                                initial_interval=timedelta(seconds=1),
                                backoff_coefficient=2.0,
                                maximum_interval=timedelta(seconds=60),
                                maximum_attempts=5,
                            ),
                        )

                        if data:
                            await workflow.execute_activity(
                                load_opportunities_to_snowflake,
                                data,
                                start_to_close_timeout=timedelta(seconds=60),
                            )

                        data = await workflow.execute_activity(
                            activities,
                            prospect_id,
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(
                                initial_interval=timedelta(seconds=1),
                                backoff_coefficient=2.0,
                                maximum_interval=timedelta(seconds=60),
                                maximum_attempts=5,
                            ),
                        )

                        if data:
                            await workflow.execute_activity(
                                load_activities_to_snowflake,
                                data,
                                start_to_close_timeout=timedelta(seconds=60),
                            )

                        data = await workflow.execute_activity(
                            tasks,
                            prospect_id,
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(
                                initial_interval=timedelta(seconds=1),
                                backoff_coefficient=2.0,
                                maximum_interval=timedelta(seconds=60),
                                maximum_attempts=5,
                            ),
                        )

                        if data:
                            await workflow.execute_activity(
                                load_tasks_to_snowflake,
                                data,
                                start_to_close_timeout=timedelta(seconds=60),
                            )

                        data = await workflow.execute_activity(
                            sales_activities,
                            prospect_id,
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(
                                initial_interval=timedelta(seconds=1),
                                backoff_coefficient=2.0,
                                maximum_interval=timedelta(seconds=60),
                                maximum_attempts=5,
                            ),
                        )

                        if data:
                            await workflow.execute_activity(
                                load_sales_activities_to_snowflake,
                                data,
                                start_to_close_timeout=timedelta(seconds=60),
                            )

                        await workflow.sleep(1)

        data, user_ids = await workflow.execute_activity(
            users,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(seconds=60),
                maximum_attempts=5,
            ),
        )

        if data:
            await workflow.execute_activity(
                load_users_to_snowflake,
                data,
                start_to_close_timeout=timedelta(seconds=60),
            )

        if len(user_ids) > 0:
            for user_id in user_ids:
                data = await workflow.execute_activity(
                    user_by_id,
                    user_id,
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=1),
                        backoff_coefficient=2.0,
                        maximum_interval=timedelta(seconds=60),
                        maximum_attempts=5,
                    ),
                )

                if data:
                    await workflow.execute_activity(
                        load_user_by_id_to_snowflake,
                        data,
                        start_to_close_timeout=timedelta(seconds=60),
                    )

                await workflow.sleep(1)

        pages = await workflow.execute_activity(
            total_email_campaign_stats_pages,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(seconds=60),
                maximum_attempts=3,
            ),
        )

        if pages >= 1:
            for page_number in range(1, pages):
                data = await workflow.execute_activity(
                    email_campaign_stats,
                    page_number,
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=1),
                        backoff_coefficient=2.0,
                        maximum_interval=timedelta(seconds=60),
                        maximum_attempts=5,
                    ),
                )

                if data:
                    await workflow.execute_activity(
                        load_email_campaign_stats_to_snowflake,
                        data,
                        start_to_close_timeout=timedelta(seconds=60),
                    )

                await workflow.sleep(1)
