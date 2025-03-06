-- ========================================
-- Table: establishments
-- ========================================
CREATE TABLE establishments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    practo_uuid VARCHAR(255) UNIQUE NOT NULL,  -- Mapped from "practo_id"
    name VARCHAR(255) NOT NULL,  -- Mapped from "name"
    slug VARCHAR(255) NOT NULL,  -- Mapped from "slug"
    practice_type VARCHAR(100),  -- Mapped from "practice_type" - clinic, hospital, etc.
    active BOOLEAN DEFAULT true,  -- Mapped from "status" (Assuming active if status is available)
    profile_url TEXT,  -- Mapped from "profile_url"
    image_url TEXT,  -- Mapped from "image_url"
    street_address TEXT,  -- Mapped from "address_line1" and "address_line2"
    city VARCHAR(100),  -- Mapped from "city"
    state VARCHAR(50),  -- Mapped from "state"
    postal_code VARCHAR(20),  -- Mapped from "zipcode"
    locality VARCHAR(255),  -- Mapped from "locality"
    latitude DECIMAL(9,6),  -- Mapped from "latitude"
    longitude DECIMAL(9,6),  -- Mapped from "longitude"
    min_price DECIMAL(10,2),  -- Mapped from "min_price"
    max_price DECIMAL(10,2),  -- Mapped from "max_price"
    phone VARCHAR(50),  -- Mapped from "phone_number"
    phone_extension VARCHAR(10),  -- Mapped from "phone_extension"
    rating DECIMAL(3,2),  -- Mapped from "rating"
    reviews_count INT,  -- Mapped from "reviews_count"
    practice_timings TEXT,  -- Mapped from "practice_timings"
    doctor_count INT,  -- Mapped from "doctor_count"

    -- Fields from reference schema but not in provided API response, can be scraped from the website
    number_of_beds INT,  -- Not present in input data
    number_of_ambulances INT,  -- Not present in input data
    
    -- Additional fields from reference schema but not mapped:
    -- fhir_id VARCHAR(64) UNIQUE,  -- Not in input data
    -- alias TEXT[],  -- Not in input data
    -- type_code VARCHAR(50),  -- Not in input data
    -- type_system VARCHAR(255),  -- Not in input data
    -- email VARCHAR(255)[],  -- Not in input data
    -- website VARCHAR(255),  -- Not in input data
    -- country VARCHAR(50),  -- Not in input data
    -- established_on DATE,  -- Not in input data

    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- Table: doctors
-- ========================================
CREATE TABLE doctors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    practo_uuid VARCHAR(255) UNIQUE NOT NULL,  -- Mapped from "practo_id"
    active BOOLEAN DEFAULT true,  -- Assuming active status
    first_name VARCHAR(100),  -- Extracted from "name" (Splitting logic needed for full name)
    last_name VARCHAR(100),  -- Extracted from "name" (Splitting logic needed for full name)
    profile_photo TEXT,  -- Mapped from "profile_photo"
    profile_url TEXT,  -- Mapped from "profile_url"
    slug VARCHAR(255) NOT NULL,  -- Mapped from "slug"
    practo_rank INT,  -- Mapped from "practo_rank"
    qualifications TEXT[],  -- Mapped from "qualifications", list of qualifications - when where what
    specialization TEXT,  -- Mapped from "specialization"
    specialties TEXT,  -- Mapped from "specialties", list of specialties
    experience_years INT,  -- Mapped from "experience_years"
    summary TEXT,  -- Mapped from "summary"
    services TEXT[],  -- Mapped from "Services", list of services
    services_count INT,  -- Mapped from "services_count"
    recommendation_percent DECIMAL(5,2),  -- Mapped from "recommendation_percent"
    patients_count INT,  -- Mapped from "patients_count"
    reviews_count INT,  -- Mapped from "reviews_count"
    establishment_count INT,  -- Mapped from "establishment_count"

    -- Fields from reference schema but not in provided data:
    -- npi VARCHAR(50) UNIQUE,  -- Not present in input data
    -- fhir_id VARCHAR(64) UNIQUE,  -- Not in input data
    -- middle_name VARCHAR(100),  -- Not in input data
    -- prefix VARCHAR(20)[],  -- Not in input data
    -- suffix VARCHAR(20)[],  -- Not in input data
    -- gender VARCHAR(20),  -- Not in input data
    -- birth_date DATE,  -- Not in input data
    -- license_number VARCHAR(50),  -- Not in input data
    -- license_state VARCHAR(50),  -- Not in input data
    -- phone VARCHAR(50)[],  -- Not in input data
    -- email VARCHAR(255)[],  -- Not in input data

    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- Table: doctor_establishment (Many-to-Many)
-- ========================================
CREATE TABLE doctor_establishment (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doctor_id UUID NOT NULL REFERENCES doctors(id) ON DELETE CASCADE,
    establishment_id UUID NOT NULL REFERENCES establishments(id) ON DELETE CASCADE,
    active BOOLEAN DEFAULT true,  -- Mapped from "status"
    begin_time TIME,  -- Start time
    end_time TIME,    -- End time
    available_days TEXT[],  -- Array of available days (e.g., ['Monday', 'Tuesday'])
    fees TEXT[],  -- Mapped from "fees" (List of amount, type)
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
);

-- ========================================
-- Trigger Function for Updating Timestamps
-- ========================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- Triggers for Auto-updating 'updated_at'
-- ========================================
CREATE TRIGGER trg_update_establishments_updated_at
BEFORE UPDATE ON establishments
FOR EACH ROW
EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER trg_update_doctors_updated_at
BEFORE UPDATE ON doctors
FOR EACH ROW
EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER trg_update_doctor_establishment_updated_at
BEFORE UPDATE ON doctor_establishment
FOR EACH ROW
EXECUTE PROCEDURE update_updated_at_column();

-- ========================================
-- Indexes for Performance
-- ========================================
CREATE INDEX idx_establishments_slug ON establishments(slug);
CREATE INDEX idx_doctors_slug ON doctors(slug);
CREATE INDEX idx_doctor_establishment_doctor_id ON doctor_establishment(doctor_id);
CREATE INDEX idx_doctor_establishment_establishment_id ON doctor_establishment(establishment_id);
