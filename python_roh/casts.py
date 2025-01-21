import requests

from cloud.utils import log
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
    cast_df.name = cast_df.name.str.strip()
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
    log("Processing new past casts")
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
        log("No new past events to process")
        return

    log(
        f"Processing {len(new_past_events_df)} new past events: {new_past_events_df.title.unique()}"
    )

    cast_dfs = []
    for performance_id in new_past_performance_ids:
        cast_df = try_get_cast_for_current_performance(performance_id)
        if cast_df.empty:
            continue
        cast_dfs.append(cast_df)

    if not cast_dfs:
        log("No new cast data to process")
        return

    cast_df = pd.concat(cast_dfs, ignore_index=True)

    new_existing_casts = pd.concat([existing_casts, cast_df], ignore_index=True)
    new_existing_casts.name = new_existing_casts.name.str.strip()
    Parquet(CASTS_PARQUET_LOCATION).write(new_existing_casts)
    log(f"Saved {len(cast_df)} new cast entries to parquet: {cast_df.slug.unique()}")
    return cast_df


def handle_seen_performances():
    full_events_df = Parquet(EVENTS_PARQUET_LOCATION).read()
    seen_performances = Firestore(SEEN_PERFORMANCES_LOCATION).read()
    casts_df = Parquet(CASTS_PARQUET_LOCATION).read(use_bigquery=False)
    e_df = full_events_df.query("location == 'Main Stage'")
    e_df = e_df.assign(timestamp_str=e_df.timestamp.dt.strftime("%Y-%m-%d %H:%M"))

    seen_df = e_df.query("timestamp_str in @seen_performances")
    seen_df = seen_df[["timestamp_str", "performanceId"]]
    di = seen_df.set_index("timestamp_str").to_dict()
    di = di["performanceId"]

    seen_performance_ids = list(di.values())
    seen_events_df = load_seen_events_df(seen_performance_ids)
    seen_casts_df = get_seen_casts(casts_df, seen_events_df)

    # Add title and timestamp to seen casts
    performances_df = seen_events_df.loc[:, ["title", "timestamp", "performanceId"]]
    performances_df = performances_df.drop_duplicates()
    seen_casts_df = seen_casts_df.merge(
        performances_df, left_on="performance_id", right_on="performanceId", how="inner"
    )
    seen_casts_df = seen_casts_df.drop(columns=["performanceId"], errors="ignore")

    Firestore(SEEN_EVENTS_PARQUET_LOCATION).write(seen_events_df)
    Firestore(SEEN_CASTS_PARQUET_LOCATION).write(seen_casts_df)

    return full_events_df


def load_casts_df():
    casts_df = Parquet(CASTS_PARQUET_LOCATION).read(use_bigquery=False)
    return casts_df


def load_seen_events_df(seen_performance_ids):
    seen_events_df = Parquet(EVENTS_PARQUET_LOCATION).read(
        filters=[
            ("location", "=", "Main Stage"),
            ("title", "!=", "Friends Rehearsals"),
            ("performanceId", "in", seen_performance_ids),
        ],
        use_bigquery=True,
    )
    return seen_events_df


def get_seen_casts(seen_casts_df, seen_events_df):
    seen_casts = Parquet(CASTS_PARQUET_LOCATION).read()
    seen_casts = seen_casts_df.query("performance_id in @seen_events_df.performanceId")
    # This column doesn't exist in the parquet, only in the "seen" casts in Firestore
    seen_casts = seen_casts.drop(columns=["timestamp"], errors="ignore")
    return seen_casts


def get_previously_seen_casts(casts_df, seen_casts_df):
    seen_casts_df = seen_casts_df.merge(
        casts_df, on="name", how="inner", suffixes=("_seen", "")
    )
    common_names = [
        "Orchestra of the Royal Opera House",
        "Royal Opera Chorus",
        # "William Spaulding",
        # "Vasko Vassilev",
    ]
    seen_casts_df = seen_casts_df.query("name not in @common_names")
    seen_casts_df = seen_casts_df[
        [
            "name",
            "performance_id_seen",
            "title",
            "timestamp",
            "role_seen",
            "is_replacing_seen",
        ]
    ]
    return seen_casts_df
