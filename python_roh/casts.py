import requests
from python_roh.src.config import *
from tools import Parquet, Firestore


def try_get_cast_for_current_performance(performance_id):
    url = f"https://www.rbo.org.uk/api/account-activities?ids={performance_id}"
    cast_page = requests.get(url)
    try:
        cast_page_json = cast_page.json()
    except ValueError:
        return None
    data = cast_page_json.get("data")
    if not data:
        return None
    included = cast_page_json.get("included")
    cast_df_og = pd.DataFrame(included)
    attributes = cast_df_og.attributes.apply(pd.Series)
    cast_df_og = pd.concat([cast_df_og, attributes], axis=1)
    cast_df = cast_df_og.query("type == 'accountCast'")
    if cast_df.empty:
        return None
    cols_to_drop = ["type", "relationships", "title", "image"]
    cast_df = cast_df.drop(columns=cols_to_drop, errors="ignore")
    cast_df = cast_df.drop(columns=["attributes", "slug"], errors="ignore")
    cast_df = cast_df.assign(is_replacing=cast_df.id.str.contains("replaced"))
    cast_df = cast_df.assign(replaced_name=None)
    replaced_df = cast_df.query("is_replacing")
    replacement_df = cast_df.query("id.str.contains('-replacement')")
    cast_df = cast_df.query("~id.str.contains('-replacement')")
    cast_df = cast_df.query("~is_replacing")
    roles = replacement_df.role.str.replace("R ", "")
    replacement_df = replacement_df.assign(role=roles)
    replacement_df = replacement_df[["role", "name"]]
    replaced_df = replaced_df.merge(replacement_df, on="role", how="left")
    replaced_df = replaced_df.rename(columns={"name_y": "name"})
    replaced_df = replaced_df.assign(replaced_name=replaced_df.name_x)
    replaced_df = replaced_df.drop(columns=["name_x"], errors="ignore")
    cast_df = pd.concat([cast_df, replaced_df], ignore_index=True)
    cast_df = cast_df.drop(columns=["id"], errors="ignore")
    cast_df = cast_df.assign(url=url, performance_id=performance_id)
    try:
        slug = cast_df_og.query("type == 'accountEvent'").iloc[0].slug
    except Exception:
        slug = None
    cast_df = cast_df.assign(slug=slug, season_id=None)
    return cast_df


def handle_new_past_casts(events_df):
    existing_casts = Parquet(CASTS_PARQUET_LOCATION).read(use_bigquery=False)
    known_uncast_events = Firestore(MISSING_CASTS_LOCATION).read()
    time_now = pd.Timestamp.now(tz="Europe/London") - pd.Timedelta("2D")
    past_events_df = events_df.query(
        "timestamp < @time_now and location == 'Main Stage' and title != 'Friends Rehearsals'"
    )
    cast_performance_ids = set(existing_casts.performance_id)
    past_performance_ids = set(past_events_df.performanceId)
    known_uncast_performance_ids = set(known_uncast_events)
    new_past_performance_ids = (
        past_performance_ids - cast_performance_ids - known_uncast_performance_ids
    )
    new_past_events_df = past_events_df.query(
        "performanceId in @new_past_performance_ids"
    )
    if new_past_events_df.empty:
        print("No new past events to process")
        return

    print(
        f"Processing {len(new_past_events_df)} new past events: {new_past_events_df.title.unique()}"
    )

    cast_dfs = []
    for performance_id in new_past_performance_ids:
        cast_df = try_get_cast_for_current_performance(performance_id)
        if cast_df.empty:
            continue
        cast_dfs.append(cast_df)

    if not cast_dfs:
        print("No new cast data to process")
        return

    cast_df = pd.concat(cast_dfs, ignore_index=True)

    new_existing_casts = pd.concat([existing_casts, cast_df], ignore_index=True)
    Parquet(CASTS_PARQUET_LOCATION).write(new_existing_casts)
    print(f"Saved {len(cast_df)} new cast entries to parquet: {cast_df.slug.unique()}")
    return cast_df
