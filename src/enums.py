PROJECT_ID = "recove-450509"
DATASET_ID = "vinted"

ITEM_TABLE_ID = "item"
SOLD_TABLE_ID = "sold"
LIKES_TABLE_ID = "likes"
PINECONE_TABLE_ID = "pinecone"
VINTED_ID_FIELD = "vinted_id"
AVAILABLE_FIELD = "is_available"

PINECONE_INDEX_NAME = "items"

BS4_PARSER = "html.parser"
REQUESTS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

SOLD_CONTAINER_TYPE = "div"
SOLD_CONTAINER_ATTRS = {"data-testid": "item-status--content"}
SOLD_STATUS_CONTENT = "Vendu"

NOT_FOUND_CONTAINER_TYPE = "h1"
NOT_FOUND_CONTAINER_ATTRS = {
    "class": "web_ui__Text__text web_ui__Text__heading web_ui__Text__center"
}
NOT_FOUND_STATUS_CONTENT = "La page n'existe pas"

INVALID_BRANDS = ["", "Vintage Dressing", "Sonstiges"]
