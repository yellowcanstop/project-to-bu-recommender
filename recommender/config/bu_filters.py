# filter by project state. no need filter by region
BU_FILTERS = {
    "makna_setia": {
        "category": ["infrastructure", "transport"],
        "subcategory": ["bridge", "expressway", "highway", "infrastructure", "lrt", "mrt"],
        "project_status": ["tender called - tenderers listed"],      
        "project_stage": ["pre-construction"],
        "project_region": ["central region", "northern region", "southern region", "east coast", "east malaysia"],
        "project_state": ["kuala lumpur", "putrajaya", "selangor", "perlis", "kedah", "penang", "perak", "negeri sembilan", "malacca", "johor", "terengganu", "kelantan", "pahang", "sabah", "sarawak"],
        "start_date_min": "2026-01-01",
        "end_date_min": "2028-01-01",
        "min_value": 100_000_000,
    },
    "fiamma": {
        "category": ["hospitality", "residential"],
        "subcategory": ["hotel", "serviced apartment", "apartment, condominium, townhouse", "house, villa, bungalow"],
        "subcategory_min_units": {
          "hotel": 100,
          "house, villa, bungalow": 100
        },
        "project_status": ["design tender", "design approval", "tender called - tenderers listed", "contract awarded / builder appointed", "subcontractor tender called", "subcontractor tenderers listed"],      
        "project_stage": ["concept", "design & documentation",  "pre-construction", "construction"],
        "development_type": ["interior fitout", "new construction", "renovation"],
        "development_type_min_units": {
          "interior fitout": 100,
          "renovation": 100
        },
        "project_region": ["central region", "northern region", "southern region", "east malaysia"],
        "project_state": ["kuala lumpur", "putrajaya", "selangor", "penang", "perak", "negeri sembilan", "malacca", "johor", "sabah", "sarawak"],
        "start_date_min": "2025-01-01",
        "end_date_min": "2027-01-01",
        "min_value": 10_000_000,
    },
    "ppch": {
        "project_status": ["contract awarded / builder appointed", "subcontractor tender called", "subcontractor tenderers listed", "subcontractor tender closed", "subcontractor(s) appointed", "main contractor on site", "site works commenced", "construction commenced"],      
        "project_stage": ["pre-construction", "construction"],
        "development_type": ["alterations & additions", "extension", "interior fitout", "maintenance", "new construction", "refurbishment", "rehabilitation", "restoration / reinstatement", "renovation", "resurfacing", "supply contract", "upgrading"],
        #"project_region": ["central region", "northern region", "southern region", "east coast", "east malaysia"],
        #"project_state": ["kuala lumpur", "putrajaya", "selangor", "perlis", "kedah", "penang", "perak", "negeri sembilan", "malacca", "johor", "terengganu", "kelantan", "pahang", "labuan", "sabah", "sarawak"],
        "start_date_min": "2025-01-01",
        "start_date_max": "2026-12-31",
        "end_date_min": "2026-01-01"
    },
    "starken aac": {
        "category": ["community & cultural", "education", "health", "hospitality", "industrial", "legal & military", "office", "residential", "retail", "sport & recreation", "transport"],
        "subcategory": ["conference centre", "community hall, community centre", "exhibition centre", "fire station, emergency centre", "funeral parlour, crematorium, cemetery", "gallery, museum", "library", "religious building", "theatre, cinema", "childcare centre, kindergarten", "research centre, lab, observatory", "school", "university, college, institute", "aged care family", "hospital", "medical centre", "nursing home", "veterinary clinic", "caravan park, camping ground", "hotel", "motel, hostel, backpacking", "resort", "serviced apartment", "agricultural, horticultural facility", "assembly plant", "factory", "food & beverage processing", "manufacturing & processing", "paper & printing", "warehousing & logistics", "workshop, garage", "law court", "military housing, college", "military installations, facilities", "police station", "prison", "commercial complex", "data centre", "embassy", "government office", "office", "technology park, call centre", "apartment, condominium, townhouse", "dormitory", "house, villa, bungalow", "residential estate", "retirement village", "urban development", "petrol station", "shop, shopping centre, supermarket", "showroom, retail warehouse, market", "stadium, grandstand, pavillion", "air control tower", "freight terminal", "passenger terminal"],
        "project_status": ["design contract awarded", "design approval", "building approval", "tender called - tenderers listed", "contract awarded / builder appointed"],      
        "project_stage": ["concept", "design & documentation",  "pre-construction"],
        "development_type": ["extension", "new construction"],
        "start_date_min": "2026-01-01",
        "end_date_min": "2026-01-01",
        "min_value": 50_000_000
    }
}, 