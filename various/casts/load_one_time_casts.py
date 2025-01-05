import sys
import requests
import pandas as pd
from bs4 import BeautifulSoup
from python_roh.src.config import *
from tools import Parquet, Firestore
from python_roh.src.api import get_query_dict


def try_get_cast_for_performance(
    season_id=None,
    performance_id=None,
    slug=None,
    cast_list_href=None,
):
    url = f"https://www.rbo.org.uk/tickets-and-events/{season_id}/{slug}/cast-list/{performance_id}"
    if cast_list_href:
        url = cast_list_href
        try:
            part2 = cast_list_href.split("/tickets-and-events/")[1]
            season_id, part2 = part2.split("/", 1)
            slug, performance_id = part2.split("/cast-list/")
        except ValueError:
            return None
    print(f"Trying to get cast for {slug} {performance_id}")
    cast_page = requests.get(url)
    class_name = "sc-1wkkcn4-7"

    soup = BeautifulSoup(cast_page.content, "html.parser")
    cast_list_divs = soup.find_all("div", class_=class_name)
    if not cast_list_divs:
        return None

    casts = []
    for cast_list_div in cast_list_divs:
        roles = cast_list_div.find_all("div", class_="fsPGxl")
        names = cast_list_div.find_all("div", class_="MSzsX")
        roles = [x.text for x in roles]
        names = [x.text for x in names]

        for role, name in zip(roles, names):
            is_replacing = False
            replaced_name = None
            try:
                name, replaced_name = name.split(" replaces ")
                is_replacing = True
            except ValueError:
                pass
            try:
                name = name.split(" and ")
            except ValueError:
                pass
            try:
                name = [x.split(", ") for x in name]
                name = [item for sublist in name for item in sublist]
            except ValueError:
                pass
            try:
                name = [x.strip() for x in name]
            except ValueError:
                pass
            if len(name) == 1:
                name = name[0]
            cast = {
                "season_id": season_id,
                "performance_id": performance_id,
                "slug": slug,
                "role": role,
                "name": name,
                "is_replacing": is_replacing,
                "replaced_name": replaced_name,
                "url": url,
            }
            casts.append(cast)
    return casts


def get_historic_casts(refresh=False):
    if not refresh:
        casts_df = Parquet(HISTORIC_CASTS_PARQUET_LOCATION).read(use_bigquery=False)
        return casts_df

    productions_df = Parquet(PRODUCTIONS_PARQUET_LOCATION).read()
    events_df = Firestore(EVENTS_PARQUET_LOCATION).read(
        allow_empty=True, apply_schema=True
    )

    query_dict = get_query_dict()

    cast_sheets = [
        "https://www.rbo.org.uk/about/cast-sheets/2023-24",
        "https://www.rbo.org.uk/about/cast-sheets/2022-23",
        "https://www.rbo.org.uk/about/cast-sheets/2021-22",
    ]
    # SPLIT
    casts = []
    for cast_sheet in cast_sheets:
        cast_page = requests.get(cast_sheet)
        # JS equivalent: document.querySelectorAll('.sc-175uz2x-161 ul')
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(cast_page.content, "html.parser")
        cast_list_uls = soup.find_all("ul")
        cast_list_as = [x.find_all("a") for x in cast_list_uls if hasattr(x, "a")]
        cast_list_as = [item for sublist in cast_list_as for item in sublist]
        cast_list_hrefs = [x["href"] for x in cast_list_as if hasattr(x, "href")]
        cast_list_hrefs = [x for x in cast_list_hrefs if "/cast-list/" in x]
        cast_list_hrefs = [x for x in cast_list_hrefs if "/tickets-and-events/" in x]
        # Get season IDs, e.g. 43
        # season_ids = [x.split("tickets-and-events/")[1] for x in cast_list_hrefs]
        # season_ids = [x.split("/")[0] for x in season_ids]

        # season_id = [x for x in set(season_ids) if x.isdigit()][0]
        # slugs = [x.split("/cast-list/")[0] for x in cast_list_hrefs]
        # slugs = list(set([x.split("/")[-1] for x in slugs]))
        # performance_ids = [x.split("/cast-list/")[1] for x in cast_list_hrefs]

        # SPLIT OUT

        for cast_list_href in cast_list_hrefs:
            print(f"Getting cast for {cast_list_href}")
            cast = try_get_cast_for_performance(cast_list_href=cast_list_href)
            if cast:
                casts.extend(cast)

    casts_df = pd.DataFrame(casts)
    casts_df = casts_df.explode("name")

    if refresh:
        result = Parquet(HISTORIC_CASTS_PARQUET_LOCATION).write(casts_df)

    # cast_list_href == f"..../{slug}/..."
    return casts


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


def get_current_casts(uncast_events_df, refresh=False):
    if not refresh:
        casts_df = Parquet(CURRENT_CASTS_PARQUET_LOCATION).read(use_bigquery=False)
        return casts_df
    performance_ids = uncast_events_df.performanceId.unique()
    casts = []
    for performance_id in performance_ids:
        print(f"Getting cast for {performance_id}")
        cast = try_get_cast_for_current_performance(performance_id)
        if cast is not None and not cast.empty:
            casts.append(cast)
    casts_df = pd.concat(casts, ignore_index=True)
    if refresh:
        Parquet(CURRENT_CASTS_PARQUET_LOCATION).write(casts_df)
    return casts_df


def store_uncast_historic_performances(historic_casts_df, current_casts_df, events_df):
    historic_uncast_ids = set(historic_casts_df.performance_id)
    current_uncast_ids = set(current_casts_df.performance_id)
    all_uncast_ids = set(events_df.performanceId)
    uncast_performance_ids = all_uncast_ids - historic_uncast_ids - current_uncast_ids
    Firestore(MISSING_CASTS_LOCATION).write(uncast_performance_ids)
    return uncast_performance_ids


def get_other_casts(historic_casts_df=None, refresh=False):
    events_df = Parquet(EVENTS_PARQUET_LOCATION).read()
    events_df = events_df.query("title != 'Friends Rehearsals'")
    time_now = pd.Timestamp.now(tz="Europe/London") - pd.Timedelta("2D")
    events_df = events_df.query("location == 'Main Stage' and timestamp < @time_now")
    if historic_casts_df is None:
        existing_performance_ids = set()
    else:
        existing_performance_ids = set(historic_casts_df.performance_id)
    all_performance_ids = set(events_df.performanceId)
    uncast_performance_ids = all_performance_ids - existing_performance_ids
    uncast_events_df = events_df.query("performanceId in @uncast_performance_ids")
    current_casts_df = get_current_casts(uncast_events_df, refresh=refresh)
    store_uncast_historic_performances(historic_casts_df, current_casts_df, events_df)

    return current_casts_df


def get_one_time_past_performance_casts(refresh=False, **kwargs):
    historic_casts_df = get_historic_casts(refresh=refresh)
    current_casts_df = get_other_casts(
        historic_casts_df=historic_casts_df, refresh=refresh
    )
    casts_df = pd.concat([historic_casts_df, current_casts_df], ignore_index=True)
    Parquet(CASTS_PARQUET_LOCATION).write(casts_df)
    return casts_df


if __name__ == "__main__":
    args = sys.argv[1:]
    refresh = args[0] == "refresh" if args else False
    casts_df = get_one_time_past_performance_casts(refresh=refresh)
    breakpoint()
