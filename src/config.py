import os

ZONE_HIERARCHY = {
    "Orchestra Stalls": 0,
    "Stalls Circle": 1,
    "Donald Gordon Grand Tier": 2,
    "Balcony": 3,
}

SEATS_BASE_URL = f"https://www.roh.org.uk/api/proxy/TXN/Performances/{os.environ['PERFORMANCE_ID']}/Seats"
PRICE_BASE_URL = f"https://www.roh.org.uk/api/proxy/TXN/Performances/Prices"
PRICE_TYPES_BASE_URL = "https://www.roh.org.uk/api/proxy/TXN/PriceTypes/Details"
ZONE_ID_BASE_URL = (
    f"https://www.roh.org.uk/api/proxy/TXN/Performances/ZoneAvailabilities"
)
