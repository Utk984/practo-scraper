import os

from dotenv import load_dotenv

load_dotenv()

# Database Connection
DATABASE_URL = os.getenv("DATABASE_URL")

# API URLs
CLINICS_URL = "https://www.practo.com/marketplace-api/dweb/listing/clinic-seo/v2?ad_limit=2&platform=desktop_web&sapphire=true&topaz=true&with_ad=true&with_seo_data=true&reach_version=v4&city={city}&url_path={url_path}&query_type=clinic%20speciality&placement=CLINIC_SEARCH&page=1"
HOSPITALS_URL = "https://www.practo.com/marketplace-api/dweb/listing/hospital-seo/v2?ad_limit=2&platform=desktop_web&sapphire=true&topaz=true&with_ad=true&with_seo_data=true&reach_version=v4&city={city}&url_path={url_path}&query_type=hospital%20speciality&placement=HOSPITAL_SEARCH&page=1"
DOCTORS_URL = "https://www.practo.com/marketplace-api/dweb/search/provider-seo/v2/?url_path={url_path}&page=1&reach_version=v4&ad_limit=2&platform=desktop_web&topaz=true&with_seo_data=true&city={city}&enable_partner_listing=true&speciality={speciality}&placement=DOCTOR_SEARCH&is_procedure_cost_page=false&show_new_reach_card=true&with_ad=true"

ESTABLISHMENT_PROFILE_URL = "https://www.practo.com/marketplace-api/dweb/profile/establishment/provider-relation-paginated?establishmentSlug={slug}&platform=desktop_web"
DOCTOR_PROFILE_URL = "https://www.practo.com/marketplace-api/dweb/profile/provider/relation?profile_slug={slug}&profile_type=doctor&platform=desktop_web&slug={slug}"

# Database Columns
DOCTOR_COLUMNS = [
    "practo_uuid",
    "slug",
    "practo_rank",
    "profile_photo",
    "profile_url",
    "first_name",
    "last_name",
    "qualifications",
    "specialization",
    "specialties",
    "experience_years",
    "summary",
    "services",
    "services_count",
    "recommendation_percent",
    "patients_count",
    "reviews_count",
    # "establishment_count",
]

ESTABLISHMENT_COLUMNS = [
    "practo_uuid",
    "name",
    "slug",
    "practice_type",
    "profile_url",
    "image_url",
    "street_address",
    "postal_code",
    "locality",
    "city",
    "state",
    "latitude",
    "longitude",
    "min_price",
    "max_price",
    "phone",
    "phone_extension",
    "rating",
    "reviews_count",
    "practice_timings",
    # "doctor_count",
]


# SQL Queries
def generate_do_update_clause(columns):
    return ", ".join(
        [
            f"{col} = EXCLUDED.{col}"
            for col in columns
            if col
            not in ["practo_uuid", "state", "establishment_count", "doctor_count"]
        ]
    )


DOCTOR_DO_UPDATE_CLAUSE = generate_do_update_clause(DOCTOR_COLUMNS)
ESTABLISHMENT_DO_UPDATE_CLAUSE = generate_do_update_clause(ESTABLISHMENT_COLUMNS)

DOCTOR_INSERT_QUERY = f"""INSERT INTO practo_doctors ({', '.join(DOCTOR_COLUMNS)}) 
    VALUES ({', '.join(['%s'] * len(DOCTOR_COLUMNS))})
    ON CONFLICT (practo_uuid) DO UPDATE SET {DOCTOR_DO_UPDATE_CLAUSE};"""

ESTABLISHMENT_INSERT_QUERY = f"""INSERT INTO practo_establishments ({', '.join(ESTABLISHMENT_COLUMNS)}) 
    VALUES ({', '.join(['%s'] * len(ESTABLISHMENT_COLUMNS))})
    ON CONFLICT (practo_uuid) DO UPDATE SET {ESTABLISHMENT_DO_UPDATE_CLAUSE};"""

DOCTOR_INSERT_QUERY_SMALL = """INSERT INTO practo_doctors (
    practo_uuid, first_name, last_name, profile_photo, profile_url, slug, experience_years)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (practo_uuid) DO NOTHING;"""

ESTABLISHMENT_INSERT_QUERY_SMALL = """INSERT INTO practo_establishments (
    practo_uuid, name, slug, profile_url, city, state, locality, latitude, longitude, street_address)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (practo_uuid) DO NOTHING;"""

RELATIONS_INSERT_QUERY = """INSERT INTO practo_doctor_establishment (
    doctor_id, establishment_id, fees, begin_time, end_time, available_days) VALUES (
    (SELECT id FROM practo_doctors WHERE practo_uuid = %s),
    (SELECT id FROM practo_establishments WHERE practo_uuid = %s), 
    %s::TEXT[], %s::TIME, %s::TIME, %s::TEXT[]);"""

CHECK_EXISTENCE_QUERY = """SELECT EXISTS (SELECT 1 FROM practo_doctors WHERE practo_uuid = %s),
    EXISTS (SELECT 1 FROM practo_establishments WHERE practo_uuid = %s);"""
