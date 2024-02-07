import os
from .config import *
from python_roh.upcoming_events import query_soonest_performance_id


def get_query_dict(
    performance_id=None,
    constituent_id=None,
    mode_of_sale_id=None,
    source_id=None,
    **kwargs
):
    """
    Return the query dictionary for the API requests for use in requests.get()
    """
    mode_of_sale_id = mode_of_sale_id or os.environ["MODE_OF_SALE_ID"]
    constituent_id = constituent_id or os.environ["CONSTITUENT_ID"]
    source_id = source_id or os.environ["SOURCE_ID"]
    performance_id = performance_id or os.environ["PERFORMANCE_ID"]

    if performance_id == "soonest":
        performance_id = query_soonest_performance_id()

    os.environ["PERFORMANCE_ID"] = str(performance_id)
    os.environ["MODE_OF_SALE_ID"] = str(mode_of_sale_id)
    os.environ["CONSTITUENT_ID"] = str(constituent_id)
    os.environ["SOURCE_ID"] = str(source_id)

    query_dict = {
        "seats": {
            "url": SEATS_BASE_URL,
            "params": {
                "constituentId": constituent_id,
                "modeOfSaleId": mode_of_sale_id,
                "performanceId": performance_id,
            },
        },
        "prices": {
            "url": PRICE_BASE_URL,
            "params": {
                "expandPerformancePriceType": "",
                "includeOnlyBasePrice": "",
                "modeOfSaleId": mode_of_sale_id,
                "performanceIds": performance_id,
                "priceTypeId": "",
                "sourceId": source_id,
            },
        },
        "zone_ids": {
            "url": ZONE_ID_BASE_URL,
            "params": {
                "constituentId": constituent_id,
                "modeOfSaleId": mode_of_sale_id,
                "performanceIds": performance_id,
            },
        },
        "price_types": {
            "url": PRICE_TYPES_BASE_URL,
            "params": {
                "modeOfSaleId": mode_of_sale_id,
                "performanceIds": performance_id,
                "sourceId": source_id,
            },
        },
        "events": {
            "url": ALL_EVENTS_URL,
            "params": {},
        },
    }
    return query_dict
