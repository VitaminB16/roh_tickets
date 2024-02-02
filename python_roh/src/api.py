from .config import *


def get_query_dict(performance_id, constituent_id, mode_of_sale_id, source_id):
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
