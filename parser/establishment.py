from bs4 import BeautifulSoup

import config


def clean_numeric(value):
    """Convert empty strings, invalid numbers to None."""
    if value in ("", None):
        return None
    try:
        return (
            int(value) if isinstance(value, str) and value.isdigit() else float(value)
        )
    except ValueError:
        return None


def parse_establishment_doctor_relation(establishment_id, response, html_content):
    """Parse establishment-doctor relationship data from API response.

    Args:
        establishment_id (str): ID of the establishment
        response (dict): API response containing relationship data

    Returns:
        tuple: (parsed data dictionary, count of doctors)
    """
    data = {}
    index = 0
    doctor_count = (
        response.get("data", {})
        .get("getEstablishmentRelations", {})
        .get("total_results_count", None)
    )
    soup = BeautifulSoup(html_content, "html.parser")

    # Extract number of beds
    beds_tag = soup.find("h3", {"data-qa-id": "bed_count"})
    bed_count = beds_tag.get_text(strip=True).split("-")[-1] if beds_tag else None

    # Extract number of ambulances
    ambulances_tag = soup.find("h3", {"data-qa-id": "ambulance_count"})
    amb_count = (
        ambulances_tag.get_text(strip=True).split("-")[-1] if ambulances_tag else None
    )

    relations = (
        response.get("data", {}).get("getEstablishmentRelations", {}).get("results", [])
    )

    for relation in relations:
        value = relation.get("relation", {})
        timings = value.get("timings", [])
        provider = value.get("provider", {})

        name = provider.get("full_name", "").split(" ")
        name += [""] * (3 - len(name))

        for timing in timings:
            data[index] = {
                "relation_info": {
                    "doctor_id": str(provider.get("fabric_id", "")),
                    "establishment_id": str(establishment_id),
                    "fees": [
                        value.get("fees", [{}])[0].get("amount", None),
                        value.get("fees", [{}])[0].get("type", None),
                    ],
                    "begin_time": timing.get("begin_time", ""),
                    "end_time": timing.get("end_time", ""),
                    "available_days": timing.get("available_days", []),
                },
                "doctor_info": {
                    "doctor_id": str(provider.get("fabric_id", "")),
                    "first_name": " ".join(name[:2]),
                    "last_name": " ".join(name[2:]),
                    "profile_photo": provider.get("enhanced_image_url", ""),
                    "profile_url": provider.get("profile_url", ""),
                    "slug": provider.get("slug", ""),
                    "experience_years": provider.get("years_of_experience", None),
                },
            }
            index += 1
    return data, doctor_count, bed_count, amb_count


def parse_establishment_data(response):
    """Parse establishment data from API response.

    Args:
        response (dict): API response containing establishment data

    Returns:
        tuple: (parsed establishments data, list of slugs, SQL insert query)
    """
    establishments = response.get("establishments", {}).get("entities", {})
    establishments_data = {}
    establishments_profile = []

    for id, details in establishments.items():
        establishments_data[id] = {
            "practo_id": str(id),
            "name": details.get("name", ""),
            "slug": details.get("slug", ""),
            "practice_type": details.get("practice_type", ""),
            "profile_url": "https://www.practo.com" + details.get("profile_url", ""),
            "image_url": details.get("image_url", ""),
            "street_address": f"{str(details.get('address_line1', '')).strip()}, {str(details.get('address_line2', ''))}",
            "postal_code": details.get("zipcode"),
            "locality": details.get("locality", ""),
            "city": details.get("city", ""),
            "state": details.get("state", ""),
            "latitude": clean_numeric(details.get("latitude")),
            "longitude": clean_numeric(details.get("longitude")),
            "min_price": clean_numeric(details.get("min_price")),
            "max_price": clean_numeric(details.get("max_price")),
            "phone": details.get("vn_phone_number", {}).get("number", ""),
            "phone_extension": details.get("vn_phone_number", {}).get("extension"),
            "rating": clean_numeric(details.get("rating")),
            "reviews_count": clean_numeric(details.get("reviews_count")),
            "practice_timings": details.get("practice_timings", ""),
        }

        establishments_profile.append(
            (
                id,
                establishments_data[id]["slug"],
                establishments_data[id]["profile_url"],
            )
        )
    return (
        establishments_data,
        establishments_profile,
        config.ESTABLISHMENT_INSERT_QUERY,
    )
