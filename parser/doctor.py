import json

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


def parse_doctor_establishment_relation(doctor_id, response, html_content):
    """Parse doctor-establishment relationship data from API response.

    Args:
        doctor_id (str): ID of the doctor
        response (dict): API response containing relationship data

    Returns:
        tuple: (parsed data dictionary, count of establishments)
    """
    data = {}
    index = 0
    relations = (
        response.get("data", {}).get("providerRelations", {}).get("relations", [])
    )
    count = (
        response.get("data", {})
        .get("providerRelations", {})
        .get("establishment_count", None)
    )

    for value in relations:
        timings = value.get("timings", [])
        establishment = value.get("establishment", {})
        for timing in timings:
            data[index] = {
                "relation_info": {
                    "doctor_id": str(doctor_id),
                    "establishment_id": str(establishment.get("fabric_id", "")),
                    "fees": [
                        value.get("fees", [{}])[0].get("amount", None),
                        value.get("fees", [{}])[0].get("type", None),
                    ],
                    "begin_time": timing.get("begin_time", ""),
                    "end_time": timing.get("end_time", ""),
                    "available_days": timing.get("available_days", []),
                },
                "establishment_info": {
                    "establishment_id": str(establishment.get("fabric_id", "")),
                    "name": establishment.get("name", ""),
                    "slug": establishment.get("slug", ""),
                    "profile_url": establishment.get("profile_url", ""),
                    "city": establishment.get("address", {})
                    .get("city", {})
                    .get("city_name", ""),
                    "state": establishment.get("address", {})
                    .get("city", {})
                    .get("state_name", ""),
                    "locality": establishment.get("address", {})
                    .get("locality", {})
                    .get("name", ""),
                    "latitude": establishment.get("address", {}).get("latitude", None),
                    "longitude": establishment.get("address", {}).get(
                        "longitude", None
                    ),
                    "address": establishment.get("address", {}).get(
                        "address_line1", ""
                    ),
                },
            }
            index += 1
    return data, count, 0, 0


def parse_doctors_data(response):
    """Parse doctor data from API response.

    Args:
        response (dict): API response containing doctor data

    Returns:
        tuple: (parsed doctors data, list of slugs, SQL insert query)
    """
    doctors = response.get("doctors", {}).get("entities", {})
    doctors_data = {}
    doctors_profile = []

    for id, details in doctors.items():
        qualifications = [str(q) for q in details.get("qualifications", [])]
        specialties = [
            str(s.get("sub_specialty", "")) for s in details.get("specialties", [])
        ]
        name = details.get("doctor_name", "").split(" ")
        name += [""] * (3 - len(name))

        doctors_data[id] = {
            "practo_id": str(id),
            "slug": details.get("translated_new_slug", ""),
            "practo_rank": clean_numeric(details.get("rank", None)),
            "profile_photo": details.get("image_url", ""),
            "profile_url": "https://www.practo.com" + details.get("profile_url", ""),
            "first_name": " ".join(name[:2]).strip(),
            "last_name": " ".join(name[2:]).strip(),
            "qualifications": json.dumps(details.get("qualifications", {})),
            "specialization": details.get("specialization", ""),
            "specialties": json.dumps(details.get("specialties", {})),
            "experience_years": clean_numeric(details.get("experience_years", None)),
            "summary": details.get("summary", ""),
            "services": details.get("non_popular_services", []),
            "services_count": clean_numeric(details.get("services_count", None)),
            "recommendation_percent": clean_numeric(
                details.get("recommendation_percent", None)
            ),
            "patients_count": clean_numeric(details.get("patients_count", None)),
            "reviews_count": clean_numeric(details.get("reviews_count", None)),
        }
        doctors_profile.append(
            (id, doctors_data[id]["slug"], doctors_data[id]["profile_url"])
        )
    return doctors_data, doctors_profile, config.DOCTOR_INSERT_QUERY
