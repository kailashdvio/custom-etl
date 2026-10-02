import math
from dataclasses import dataclass
from typing import Optional

import aiohttp
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from temporalio import activity

from settings import get_settings
from utils import current_and_previous_date

settings = get_settings()

TASK_QUEUE_NAME = "leadsquared-task-queue"

LEAD_PROP_ATTRIBUTES = [
    "AvgTimePerVisit",
    "CanUpdate",
    "Company",
    "ConversionReferrerURL",
    "CreatedBy",
    "CreatedByName",
    "CreatedOn",
    "EmailAddress",
    "EngagementScore",
    "FacebookId",
    "FirstLandingPageSubmissionDate",
    "FirstLandingPageSubmissionId",
    "FirstName",
    "GTalkId",
    "GooglePlusId",
    "Groups",
    "IsLead",
    "IsStarredLead",
    "IsTaggedLead",
    "JobTitle",
    "LastName",
    "LastOptInEmailSentDate",
    "LastVisitDate",
    "Latitude",
    "LeadAge",
    "LeadConversionDate",
    "LeadLastModifiedOn",
    "LinkedInId",
    "Longitude",
    "MailingPreferences",
    "Mobile",
    "ModifiedBy",
    "ModifiedByName",
    "ModifiedOn",
    "NotableEvent",
    "NotableEventdate",
    "Notes",
    "OptInDate",
    "OptInDetails",
    "Origin",
    "OwnerId",
    "OwnerIdEmailAddress",
    "OwnerIdName",
    "PageViewsPerVisit",
    "Phone",
    "PhotoUrl",
    "ProspectActivityDate_Max",
    "ProspectActivityDate_Min",
    "ProspectActivityId_Max",
    "ProspectActivityId_Min",
    "ProspectActivityName_Max",
    "ProspectAutoId",
    "ProspectID",
    "ProspectStage",
    "QualityScore01",
    "RelatedLandingPageId",
    "RelatedProspectId",
    "Revenue",
    "Score",
    "SkypeId",
    "Source",
    "SourceCampaign",
    "SourceContent",
    "SourceIPAddress",
    "SourceMedium",
    "SourceReferrer",
    "SourceReferrerURL",
    "StatusCode",
    "StatusReason",
    "TimeZone",
    "Total",
    "TotalVisits",
    "TwitterId",
    "Web_RefKeyword",
    "Web_Referrer",
    "Website",
]

ACTIVITIES_ATTRIBUTES = [
    "Id",
    "EventCode",
    "EventName",
    "ActivityScore",
    "CreatedOn",
    "ActivityType",
    "Type",
    "IsEmailType",
    "SessionId",
    "TrackLocation",
    "Latitude",
    "Longitude",
    "RelatedProspectId",
]

SALES_ACTIVITIES_ATTRIBUTES = ["Id", "Name", "DisplayName", "Fields"]

USER_INSIGHTS_ATTRIBUTES = [
    "UserId",
    "FirstName",
    "LastName",
    "EmailAddress",
    "AssociatedPhoneNumbers",
    "DateFormat",
    "TimeZone",
    "Role",
    "IsPhoneCallAgent",
    "OrgId",
    "CompanyName",
    "UserType",
    "ManagerUserId",
    "Address",
    "City",
    "State",
    "Country",
    "ZipCode",
    "TelephonyAgentId",
    "ManagerName",
    "PhoneMain",
    "PhoneMobile",
    "PhoneOthers",
    "IsCheckedIn",
    "LastCheckedOn",
    "LastCheckedIpAddress",
    "AvailabilityStatus",
    "Designation",
    "Department",
    "OfficeLocationName",
    "Skills",
    "SalesRegions",
    "mx_Custom_1",
    "mx_Custom_2",
    "mx_Custom_3",
    "mx_Custom_4",
    "mx_Custom_5",
    "mx_Custom_6",
    "mx_Custom_7",
    "mx_Custom_8",
    "mx_Custom_9",
    "mx_Custom_10",
    "Groups",
    "TeamId",
    "TeamName",
]

TASKS_ATTRIBUTES = [
    "TaskID",
    "Name",
    "Description",
    "DueDate",
    "CreatedOn",
    "OwnerID",
    "OwnerName",
    "CreatedBy",
    "CreatedByName",
    "Status",
    "Reminder",
]

EMAIL_CAMPAIGN_STATS = [
    "Id",
    "CampaignName",
    "Subject",
    "TotalRecipients",
    "ScheduledOn",
    "NoAction",
    "Delivered",
    "Open",
    "Click",
    "Bounce",
    "Unsubscribed",
    "Spam",
    "FromUser",
    "FromUserName",
    "CreatedBy",
    "CreatedByName",
]


@dataclass
class LeadProperties:
    attribute: str
    value: Optional[str]


current_date, previous_date = current_and_previous_date()


@activity.defn
async def total_lead_properties_pages():
    async with aiohttp.ClientSession() as session:
        headers = {
            "x-LSQ-SecretKey": settings.LSQ_SECRET_KEY,
            "x-LSQ-AccessKey": settings.LSQ_ACCESS_KEY,
            "Content-Type": "application/json",
        }

        page_number = 1
        page_size = 100
        total_pages = None

        payload = {
            "Parameter": {
                "FromDate": previous_date,
                "ToDate": current_date,
            },
            "Paging": {"PageIndex": page_number, "PageSize": page_size},
            "Sorting": {"ColumnName": "ProspectAutoId", "Direction": "1"},
        }

        async with session.post(
            f"{settings.HOST}/LeadManagement.svc/Leads.RecentlyModified",
            headers=headers,
            json=payload,
        ) as response:
            if not response.ok:
                raise RuntimeError(f"Status: {response.status}")

            response_body = await response.json()
            record_count = response_body.get("RecordCount", 0)

            if total_pages is None:
                total_pages = math.ceil(record_count / page_size)
        return total_pages


@activity.defn
async def lead_properties(page_number):
    async with aiohttp.ClientSession() as session:
        results = []

        headers = {
            "x-LSQ-SecretKey": settings.LSQ_SECRET_KEY,
            "x-LSQ-AccessKey": settings.LSQ_ACCESS_KEY,
            "Content-Type": "application/json",
        }

        page_size = 100
        attributes = []
        prospect_ids = []

        payload = {
            "Parameter": {
                "FromDate": previous_date,
                "ToDate": current_date,
            },
            "Paging": {"PageIndex": page_number, "PageSize": page_size},
            "Sorting": {"ColumnName": "ProspectAutoId", "Direction": "1"},
        }

        async with session.post(
            f"{settings.HOST}/LeadManagement.svc/Leads.RecentlyModified",
            headers=headers,
            json=payload,
        ) as response:
            if not response.ok:
                raise RuntimeError(f"Status: {response.status}")

            response_body = await response.json()
            leads = response_body.get("Leads", [])

        index_prospect_id = -1
        counter = 0

        for lead in leads:
            lead_properties = lead.get("LeadPropertyList")
            for item in lead_properties:
                attributes.append(item.get("Attribute"))
                if item.get("Attribute") == "ProspectID":
                    index_prospect_id = counter

                counter += 1
            break

        for lead in leads:
            lead_properties = lead.get("LeadPropertyList")
            values = []
            counter = 0
            for item in lead_properties:
                values.append(
                    item.get("Value") if "Value" in item else "",
                )
                if counter == index_prospect_id:
                    prospect_ids.append(item.get("Value"))
                counter += 1

            results.append(values)

        data = {"columns": attributes, "values": results}

        return data, prospect_ids


@activity.defn
async def load_lead_properties_to_snowflake(data):
    df = pd.DataFrame(data.get("values"), columns=data.get("columns"))
    df = df.sort_index(axis=1)
    df = df[LEAD_PROP_ATTRIBUTES]

    table_name = "LEADSQUARED_PROPERTIES"
    schema_name = "LEADSQUARED_SCHEMA"

    conn = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD,
        account=settings.SNOWFLAKE_ACCOUNT,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        database=settings.SNOWFLAKE_DATABASE,
        role=settings.SNOWFLAKE_ROLE,
    )

    cs = conn.cursor()
    try:
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cs.execute(f"USE SCHEMA {schema_name}")

        columns = ", ".join([f'"{col}" STRING' for col in df.columns])
        cs.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns})")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema_name,
            overwrite=False,
        )

    finally:
        cs.close()
        conn.close()


@activity.defn
async def opportunities(prospect_id):
    async with aiohttp.ClientSession() as session:
        results = []

        headers = {
            "x-LSQ-SecretKey": settings.LSQ_SECRET_KEY,
            "x-LSQ-AccessKey": settings.LSQ_ACCESS_KEY,
            "Content-Type": "application/json",
        }

        async with session.post(
            f"{settings.HOST}/OpportunityManagement.svc/GetOpportunitiesOfLead?leadId={prospect_id}",
            headers=headers,
        ) as response:
            if not response.ok:
                raise RuntimeError(f"Status: {response.status}")

            response_body = await response.json()
            results = response_body.get("List", [])

        return results


@activity.defn
async def load_opportunities_to_snowflake(data):
    df = pd.DataFrame(data)
    df = df.sort_index(axis=1)

    table_name = "LEADSQUARED_OPPORTUNITIES"
    schema_name = "LEADSQUARED_SCHEMA"

    conn = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD,
        account=settings.SNOWFLAKE_ACCOUNT,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        database=settings.SNOWFLAKE_DATABASE,
        role=settings.SNOWFLAKE_ROLE,
    )

    cs = conn.cursor()
    try:
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cs.execute(f"USE SCHEMA {schema_name}")

        columns = ", ".join([f'"{col}" STRING' for col in df.columns])
        cs.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns})")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema_name,
            overwrite=False,
        )

    finally:
        cs.close()
        conn.close()


@activity.defn
async def activities(prospect_id):
    async with aiohttp.ClientSession() as session:
        results = []

        headers = {
            "x-LSQ-SecretKey": settings.LSQ_SECRET_KEY,
            "x-LSQ-AccessKey": settings.LSQ_ACCESS_KEY,
            "Content-Type": "application/json",
        }

        async with session.post(
            f"{settings.HOST}/ProspectActivity.svc/Retrieve?leadId={prospect_id}",
            headers=headers,
        ) as response:
            if not response.ok:
                raise RuntimeError(f"Status: {response.status}")

            response_body = await response.json()
            activities = response_body.get("ProspectActivities", [])

        for item in activities:
            id = item.get("Id")
            event_code = item.get("EventCode")
            event_name = item.get("EventName")
            activity_score = item.get("ActivityScore")
            created_on = item.get("CreatedOn")
            activity_type = item.get("ActivityType")
            type = item.get("Type")
            is_email_type = item.get("IsEmailType")
            session_id = item.get("SessionId")
            track_location = item.get("TrackLocation")
            latitude = item.get("Latitude")
            longitude = item.get("Longitude")
            related_prospect_id = item.get("RelatedProspectId")

            results.append(
                [
                    id,
                    event_code,
                    event_name,
                    activity_score,
                    created_on,
                    activity_type,
                    type,
                    is_email_type,
                    session_id,
                    track_location,
                    latitude,
                    longitude,
                    related_prospect_id,
                ]
            )

        return results


@activity.defn
async def load_activities_to_snowflake(data):
    df = pd.DataFrame(data, columns=ACTIVITIES_ATTRIBUTES)
    df = df.sort_index(axis=1)

    table_name = "LEADSQUARED_ACTIVITIES"
    schema_name = "LEADSQUARED_SCHEMA"

    conn = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD,
        account=settings.SNOWFLAKE_ACCOUNT,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        database=settings.SNOWFLAKE_DATABASE,
        role=settings.SNOWFLAKE_ROLE,
    )

    cs = conn.cursor()
    try:
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cs.execute(f"USE SCHEMA {schema_name}")

        columns = ", ".join([f'"{col}" STRING' for col in df.columns])
        cs.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns})")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema_name,
            overwrite=False,
        )

    finally:
        cs.close()
        conn.close()


@activity.defn
async def tasks(prospect_id):
    async with aiohttp.ClientSession() as session:
        results = []

        headers = {
            "x-LSQ-SecretKey": settings.LSQ_SECRET_KEY,
            "x-LSQ-AccessKey": settings.LSQ_ACCESS_KEY,
            "Content-Type": "application/json",
        }

        async with session.get(
            f"{settings.HOST}/LeadManagement.svc/RetrieveTaskByLeadId?leadId={prospect_id}",
            headers=headers,
        ) as response:
            if not response.ok:
                raise RuntimeError(f"Status: {response.status}")

            response_body = await response.json()
            tasks = response_body.get("TaskList", [])

            for item in tasks:
                task_id = item.get("TaskID")
                name = item.get("Name")
                description = item.get("Description")
                due_date = item.get("DueDate")
                created_on = item.get("CreatedOn")
                owner_id = item.get("OwnerID")
                owner_name = item.get("OwnerName")
                created_by = item.get("CreatedBy")
                created_by_name = item.get("CreatedByName")
                status = item.get("Status")
                reminder = item.get("Reminder")

                results.append(
                    [
                        task_id,
                        name,
                        description,
                        due_date,
                        created_on,
                        owner_id,
                        owner_name,
                        created_by,
                        created_by_name,
                        status,
                        reminder,
                    ]
                )

        return results


@activity.defn
async def load_tasks_to_snowflake(data):
    df = pd.DataFrame(data, columns=TASKS_ATTRIBUTES)
    df = df.sort_index(axis=1)

    table_name = "LEADSQUARED_TASKS"
    schema_name = "LEADSQUARED_SCHEMA"

    conn = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD,
        account=settings.SNOWFLAKE_ACCOUNT,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        database=settings.SNOWFLAKE_DATABASE,
        role=settings.SNOWFLAKE_ROLE,
    )

    cs = conn.cursor()
    try:
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cs.execute(f"USE SCHEMA {schema_name}")

        columns = ", ".join([f'"{col}" STRING' for col in df.columns])
        cs.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns})")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema_name,
            overwrite=False,
        )

    finally:
        cs.close()
        conn.close()


@activity.defn
async def sales_activities(prospect_id):
    async with aiohttp.ClientSession() as session:
        results = []

        headers = {
            "x-LSQ-SecretKey": settings.LSQ_SECRET_KEY,
            "x-LSQ-AccessKey": settings.LSQ_ACCESS_KEY,
            "Content-Type": "application/json",
        }

        async with session.get(
            f"{settings.HOST}/SalesActivity.svc/RetrieveByLeadId?leadId={prospect_id}&includeOption=IncludeOption",
            headers=headers,
        ) as response:
            if not response.ok:
                raise RuntimeError(f"Status: {response.status}")

            response_body = await response.json()
            sales_activities = response_body[0] if len(response_body) else {}

            for item in sales_activities:
                id = item.get("Id")
                name = item.get("Name")
                display_name = item.get("DisplayName")
                fields = str(item.get("Fields"))

                results.append([id, name, display_name, fields])

        return results


@activity.defn
async def load_sales_activities_to_snowflake(data):
    df = pd.DataFrame(data, columns=SALES_ACTIVITIES_ATTRIBUTES)
    df = df.sort_index(axis=1)

    print("DF", df)

    table_name = "LEADSQUARED_SALES_ACTIVITIES"
    schema_name = "LEADSQUARED_SCHEMA"

    conn = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD,
        account=settings.SNOWFLAKE_ACCOUNT,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        database=settings.SNOWFLAKE_DATABASE,
        role=settings.SNOWFLAKE_ROLE,
    )

    cs = conn.cursor()
    try:
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cs.execute(f"USE SCHEMA {schema_name}")

        columns = ", ".join([f'"{col}" STRING' for col in df.columns])
        cs.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns})")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema_name,
            overwrite=False,
        )

    finally:
        cs.close()
        conn.close()


@activity.defn
async def users():
    async with aiohttp.ClientSession() as session:
        results = []
        user_ids = []

        headers = {
            "x-LSQ-SecretKey": settings.LSQ_SECRET_KEY,
            "x-LSQ-AccessKey": settings.LSQ_ACCESS_KEY,
            "Content-Type": "application/json",
        }

        async with session.get(
            f"{settings.HOST}/UserManagement.svc/Users.Get",
            headers=headers,
        ) as response:
            if not response.ok:
                raise RuntimeError(f"Status: {response.status}")

            response_body = await response.json()
            results = response_body

            for item in results:
                user_ids.append(item.get("ID"))

        return results, user_ids


@activity.defn
async def load_users_to_snowflake(data):
    df = pd.DataFrame(data)
    df = df.sort_index(axis=1)

    table_name = "LEADSQUARED_USERS"
    schema_name = "LEADSQUARED_SCHEMA"

    conn = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD,
        account=settings.SNOWFLAKE_ACCOUNT,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        database=settings.SNOWFLAKE_DATABASE,
        role=settings.SNOWFLAKE_ROLE,
    )

    cs = conn.cursor()
    try:
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cs.execute(f"USE SCHEMA {schema_name}")

        columns = ", ".join([f'"{col}" STRING' for col in df.columns])
        cs.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns})")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema_name,
            overwrite=False,
        )

    finally:
        cs.close()
        conn.close()


@activity.defn
async def user_by_id(userId):
    async with aiohttp.ClientSession() as session:
        results = []

        headers = {
            "x-LSQ-SecretKey": settings.LSQ_SECRET_KEY,
            "x-LSQ-AccessKey": settings.LSQ_ACCESS_KEY,
            "Content-Type": "application/json",
        }

        async with session.get(
            f"{settings.HOST}/UserManagement.svc/User/Retrieve/ByUserId?userId={userId}",
            headers=headers,
        ) as response:
            if not response.ok:
                raise RuntimeError(f"Status: {response.status}")

            response_body = await response.json()
            user = response_body[0] if len(response_body) > 0 else {}

        if user:
            user_id = user.get("UserId")
            first_name = user.get("FirstName")
            last_name = user.get("LastName")
            email_address = user.get("EmailAddress")
            phone_number = user.get("AssociatedPhoneNumbers")
            date_format = user.get("DateFormat")
            time_zone = user.get("TimeZone")
            role = user.get("Role")
            is_phone_call_agent = user.get("IsPhoneCallAgent")
            org_id = user.get("OrgId")
            company_name = user.get("CompanyName")
            user_type = user.get("UserType")
            manager_user_id = user.get("ManagerUserId")
            address = user.get("Address")
            city = user.get("City")
            state = user.get("State")
            country = user.get("Country")
            zip_code = user.get("ZipCode")
            telephony_agent_id = user.get("TelephonyAgentId")
            manager_name = user.get("ManagerName")
            phone_main = user.get("PhoneMain")
            phone_mobile = user.get("PhoneMobile")
            phone_others = user.get("PhoneOthers")
            is_checkin = user.get("IsCheckedIn")
            last_checked_on = user.get("LastCheckedOn")
            last_checked_ip_address = user.get("LastCheckedIpAddress")
            availability_status = user.get("AvailabilityStatus")
            designation = user.get("Designation")
            department = user.get("Department")
            office_location_name = user.get("OfficeLocationName")
            skills = user.get("Skills")
            sales_region = user.get("SalesRegions")
            mx_custom_1 = user.get("mx_Custom_1")
            mx_custom_2 = user.get("mx_Custom_2")
            mx_custom_3 = user.get("mx_Custom_3")
            mx_custom_4 = user.get("mx_Custom_4")
            mx_custom_5 = user.get("mx_Custom_5")
            mx_custom_6 = user.get("mx_Custom_6")
            mx_custom_7 = user.get("mx_Custom_7")
            mx_custom_8 = user.get("mx_Custom_8")
            mx_custom_9 = user.get("mx_Custom_9")
            mx_custom_10 = user.get("mx_Custom_10")
            groups = str(user.get("Groups"))
            team_id = user.get("TeamId")
            team_name = user.get("TeamName")

            results.append(
                [
                    user_id,
                    first_name,
                    last_name,
                    email_address,
                    phone_number,
                    date_format,
                    time_zone,
                    role,
                    is_phone_call_agent,
                    org_id,
                    company_name,
                    user_type,
                    manager_user_id,
                    address,
                    city,
                    state,
                    country,
                    zip_code,
                    telephony_agent_id,
                    manager_name,
                    phone_main,
                    phone_mobile,
                    phone_others,
                    is_checkin,
                    last_checked_on,
                    last_checked_ip_address,
                    availability_status,
                    designation,
                    department,
                    office_location_name,
                    skills,
                    sales_region,
                    mx_custom_1,
                    mx_custom_2,
                    mx_custom_3,
                    mx_custom_4,
                    mx_custom_5,
                    mx_custom_6,
                    mx_custom_7,
                    mx_custom_8,
                    mx_custom_9,
                    mx_custom_10,
                    groups,
                    team_id,
                    team_name,
                ]
            )

        return results


@activity.defn
async def load_user_by_id_to_snowflake(data):
    df = pd.DataFrame(data, columns=USER_INSIGHTS_ATTRIBUTES)
    df = df.sort_index(axis=1)

    table_name = "LEADSQUARED_USERS_INSIGHTS"
    schema_name = "LEADSQUARED_SCHEMA"

    conn = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD,
        account=settings.SNOWFLAKE_ACCOUNT,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        database=settings.SNOWFLAKE_DATABASE,
        role=settings.SNOWFLAKE_ROLE,
    )

    cs = conn.cursor()
    try:
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cs.execute(f"USE SCHEMA {schema_name}")

        columns = ", ".join([f'"{col}" STRING' for col in df.columns])
        cs.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns})")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema_name,
            overwrite=False,
        )

    finally:
        cs.close()
        conn.close()


@activity.defn
async def total_email_campaign_stats_pages():
    async with aiohttp.ClientSession() as session:
        headers = {
            "x-LSQ-SecretKey": settings.LSQ_SECRET_KEY,
            "x-LSQ-AccessKey": settings.LSQ_ACCESS_KEY,
            "Content-Type": "application/json",
        }

        page_number = 1
        page_size = 100
        total_pages = None

        payload = {
            "Paging": {"PageIndex": page_number, "PageSize": page_size},
            "Sorting": {"ColumnName": "CreatedOn", "Direction": "1"},
        }

        async with session.post(
            f"{settings.HOST}/EmailMarketing.svc/RetrieveCampaignStatistics",
            headers=headers,
            json=payload,
        ) as response:
            if not response.ok:
                raise RuntimeError(f"Status: {response.status}")

            response_body = await response.json()
            record_count = response_body.get("RecordCount", 0)

            if total_pages is None:
                total_pages = math.ceil(record_count / page_size)

        print("TOT", total_pages, response_body)

        return total_pages


@activity.defn
async def email_campaign_stats(page_number):
    async with aiohttp.ClientSession() as session:
        results = []

        headers = {
            "x-LSQ-SecretKey": settings.LSQ_SECRET_KEY,
            "x-LSQ-AccessKey": settings.LSQ_ACCESS_KEY,
            "Content-Type": "application/json",
        }
        page_size = 100
        payload = {
            "Paging": {"PageIndex": page_number, "PageSize": page_size},
            "Sorting": {"ColumnName": "CreatedOn", "Direction": "1"},
        }

        async with session.post(
            f"{settings.HOST}/EmailMarketing.svc/RetrieveCampaignStatistics",
            headers=headers,
            json=payload,
        ) as response:
            if not response.ok:
                raise RuntimeError(f"Status: {response.status}")

            response_body = await response.json()
            email_campaigns = response_body.get("List", [])

        for email_campaign in email_campaigns:
            id = email_campaign.get("Id")
            campaign_stats = email_campaign.get("CampaignName")
            subjects = email_campaign.get("Subject")
            total_recipients = email_campaign.get("TotalRecipients")
            scheduled_on = email_campaign.get("ScheduledOn")
            no_action = email_campaign.get("NoAction")
            delivered = email_campaign.get("Delivered")
            open = email_campaign.get("Open")
            click = email_campaign.get("Click")
            bounce = email_campaign.get("Bounce")
            unsubscribed = email_campaign.get("Unsubscribed")
            span = email_campaign.get("Spam")
            from_user = email_campaign.get("FromUser")
            from_user_name = email_campaign.get("FromUserName")
            created_by = email_campaign.get("CreatedBy")
            created_by_name = email_campaign.get("CreatedByName")

            results.append(
                [
                    id,
                    campaign_stats,
                    subjects,
                    total_recipients,
                    scheduled_on,
                    no_action,
                    delivered,
                    open,
                    click,
                    bounce,
                    unsubscribed,
                    span,
                    from_user,
                    from_user_name,
                    created_by,
                    created_by_name,
                ]
            )

        return results


@activity.defn
async def load_email_campaign_stats_to_snowflake(data):
    df = pd.DataFrame(data, columns=EMAIL_CAMPAIGN_STATS)
    df = df.sort_index(axis=1)

    table_name = "LEADSQUARED_EMAIL_CAMPAIGN_STATISTICS"
    schema_name = "LEADSQUARED_SCHEMA"

    conn = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD,
        account=settings.SNOWFLAKE_ACCOUNT,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        database=settings.SNOWFLAKE_DATABASE,
        role=settings.SNOWFLAKE_ROLE,
    )

    cs = conn.cursor()
    try:
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cs.execute(f"USE SCHEMA {schema_name}")

        columns = ", ".join([f'"{col}" STRING' for col in df.columns])
        cs.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns})")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema_name,
            overwrite=False,
        )

    finally:
        cs.close()
        conn.close()
