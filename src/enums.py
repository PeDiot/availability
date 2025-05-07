PROJECT_ID = "recove-450509"
VINTED_DATASET_ID = "vinted"
PROD_DATASET_ID = "prod"

ITEM_TABLE_ID = "item"
ITEM_ACTIVE_TABLE_ID = "item_active"
INDEX_TABLE_ID = "item_active_index"
SOLD_TABLE_ID = "sold"
PINECONE_TABLE_ID = "pinecone"
CLICK_OUT_TABLE_ID = "click_out"
SAVED_TABLE_ID = "saved"
VIEWED_ITEMS_TABLE_ID = "items"

SUPABASE_SAVED_TABLE_ID = "saved_item"

VINTED_ID_FIELD = "vinted_id"
AVAILABLE_FIELD = "is_available"

PINECONE_INDEX_NAME = "items"

REQUESTS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

MAX_RETRIES = 3
INITIAL_SLEEP_TIME = 10
MAX_SLEEP_TIME = 60
RATE_LIMIT_SLEEP_TIME = 30
INVALID_STATUS_CODES = [429, 403]

BS4_PARSER = "html.parser"
SOLD_CONTAINER_TYPE = "div"
SOLD_CONTAINER_ATTRS = {"data-testid": "item-status--content"}
SOLD_STATUS_CONTENT = "Vendu"

NOT_FOUND_CONTAINER_TYPE = "h1"
NOT_FOUND_CONTAINER_CLASS = (
    "web_ui__Text__text web_ui__Text__heading web_ui__Text__center"
)
NOT_FOUND_STATUS_CONTENT = "La page n'existe pas"

RATE_LIMIT_CONTAINER = "h1"
RATE_LIMIT_MESSAGE = "You are rate limited"

WAIT_HEADER_TYPE = "h1"
WAIT_HEADER_TEXT = "Please wait"
WAIT_VERIFICATION_TEXT = "Verifying you are human"
WAIT_LOADING_CLASS = "loading-verifying"


VINTAGE_DRESSING_BRAND = "Vintage Dressing"
TOP_BRANDS = [
    "Zara",
    "Nike",
    "Shein",
    "H&M",
    "adidas",
    "Local",
    "Sonstiges",
    "Bershka",
    "Ralph Lauren",
    "Pull & Bear",
    "Stradivarius",
    "Mango",
    "Primark",
    "Kiabi",
    "Tommy Hilfiger",
    "Lacoste",
    "Levi's",
    "Puma",
    "Made In Italy",
    "ONLY",
    "Pimkie",
    "Jordan",
    "Carhartt",
    "Camaïeu",
    "The North Face",
    "Jack & Jones",
    "Jennyfer",
    "Decathlon",
    "GUESS",
    "C&A",
    "Promod",
    "Fait Main",
    "Cache Cache",
    "sans marque",
    "ASOS",
    "Vintage",
    "Esprit",
    "Gucci",
    "Asics",
    "Celio",
    "Jules",
    "Vero Moda",
    "Hollister",
    "Inconnu",
    "Calvin Klein",
    "Lefties",
    "New Yorker",
    "Timberland",
    "Burberry",
    "Louis Vuitton",
    "Superdry",
    "Under Armour",
    "New Balance",
    "Massimo Dutti",
    "Naf Naf",
    "Tally Weijl",
    "Quechua",
    "Desigual",
    "Gémo",
    "Vans",
    "Bonobo",
    "Hugo Boss",
    "Springfield",
    "Terranova",
    "Boutique indépendante",
    "Champion",
    "New Collection",
    "Etam",
    "sansnom.",
    "Disney",
    "Gymshark",
    "New Era",
    "Uniqlo",
    "Diesel",
    "Reebok",
    "Prada",
    "Columbia",
    "Devred",
    "FILA",
    "Uomo",
    "Boutique Parisienne",
    "Dior",
    "Morgan",
    "Stone Island",
    "Amisu",
    "Liu Jo",
    "Salomon",
    "Dickies",
    "Scotch & Soda",
    "Converse",
    "s.Oliver",
    "Michael Kors",
    "Napapijri",
    "OVS",
    "Esmara",
    "Ray-Ban",
    "Kappa",
    "pas de marque",
    "Piazza Italia",
]
