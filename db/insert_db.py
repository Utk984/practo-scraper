import psycopg2
from psycopg2 import OperationalError, InterfaceError, DataError

import config
from utils.logger import db_logger

class DatabaseError(Exception):
    """Custom exception for database errors with detailed information."""
    def __init__(self, message, original_error=None, query=None, params=None):
        self.message = message
        self.original_error = original_error
        self.query = query
        self.params = params
        super().__init__(self.message)


def connect_db():
    """Connect to the PostgreSQL database using the URL from config.

    Returns:
        Connection object if successful

    Raises:
        DatabaseError: If connection fails
    """
    try:
        db_logger.info("Connecting to database")
        conn = psycopg2.connect(config.DATABASE_URL)
        db_logger.info("Database connection established")
        return conn
    except OperationalError as error:
        db_logger.error(f"Error connecting to database: {error}")
        raise DatabaseError(f"Failed to connect to database: {error}", original_error=error)
    except Exception as error:
        db_logger.error(f"Unexpected error connecting to database: {error}")
        raise DatabaseError(f"Unexpected error connecting to database: {error}", original_error=error)


def execute_query(query, params=None, fetch=False, fetch_one=False, commit=True):
    """Execute a database query with proper error handling.
    
    Args:
        query: SQL query to execute
        params: Parameters for the query
        fetch: Whether to fetch all results
        fetch_one: Whether to fetch a single result
        commit: Whether to commit the transaction
        
    Returns:
        Query results if fetch=True, otherwise None
        
    Raises:
        DatabaseError: If query execution fails
    """
    conn = None
    cur = None
    try:
        conn = connect_db()
        cur = conn.cursor()
        
        db_logger.debug(f"Executing query: {query}")
        if params:
            db_logger.debug(f"Query params: {params}")
        
        cur.execute(query, params)
        
        # Fetch results if requested
        result = None
        if fetch_one:
            result = cur.fetchone()
        elif fetch:
            result = cur.fetchall()
            
        # Commit if requested
        if commit:
            conn.commit()
            db_logger.debug("Transaction committed")
            
        return result
        
    except DataError as error:
        if conn:
            conn.rollback()
        db_logger.error(f"Data error executing query: {error}")
        raise DatabaseError(f"Invalid data for query: {error}", 
                           original_error=error, query=query, params=params)
    except OperationalError as error:
        if conn:
            conn.rollback()
        db_logger.error(f"Operational error executing query: {error}")
        raise DatabaseError(f"Database operation failed: {error}", 
                           original_error=error, query=query, params=params)
    except Exception as error:
        if conn:
            conn.rollback()
        db_logger.error(f"Unexpected error executing query: {error}")
        raise DatabaseError(f"Unexpected error executing query: {error}", 
                           original_error=error, query=query, params=params)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def insert_relation_data(relation, count, bed_count, amb_count, practo_id, key):
    """Insert relationship data into database.

    Args:
        relation (dict): Relationship data to insert
        count (int): Count of relationships
        bed_count (int): Count of beds (for establishments)
        amb_count (int): Count of ambulances (for establishments)
        practo_id (str): ID of the main entity (doctor/establishment)
        key (str): Type of entity ('doctor', 'hospital', or 'clinic')

    Raises:
        DatabaseError: If database operations fail
    """
    mapping = {
        "hospital": "UPDATE practo_establishments SET doctor_count = %s, number_of_beds = %s, number_of_ambulances = %s WHERE practo_uuid = %s",
        "clinic": "UPDATE practo_establishments SET doctor_count = %s, number_of_beds = %s, number_of_ambulances = %s WHERE practo_uuid = %s",
        "doctor": "UPDATE practo_doctors SET establishment_count = %s WHERE practo_uuid = %s",
    }

    db_logger.info(f"Inserting relation data for {key} with ID {practo_id}")
    
    try:
        conn = connect_db()
        cur = conn.cursor()

        # Update the count fields
        if key == "doctor":
            db_logger.debug(f"Updating doctor count: {count} for ID {practo_id}")
            cur.execute(mapping[key], (count, practo_id))
        else:
            db_logger.debug(f"Updating establishment counts: doctors={count}, beds={bed_count}, ambulances={amb_count} for ID {practo_id}")
            cur.execute(mapping[key], (count, bed_count, amb_count, practo_id))
        
        # Insert relations
        for item_id, value in relation.items():
            try:
                relations_data = value.get("relation_info", {})
                doctor_uuid = relations_data.get("doctor_id", "")
                establishment_uuid = relations_data.get("establishment_id", "")

                db_logger.debug(f"Checking existence for doctor {doctor_uuid} and establishment {establishment_uuid}")
                cur.execute(config.CHECK_EXISTENCE_QUERY, (doctor_uuid, establishment_uuid))
                doctor_exists, establishment_exists = cur.fetchone()

                # Insert doctor if not exists
                if not doctor_exists and "doctor_info" in value:
                    doctor_data = value.get("doctor_info")
                    db_logger.debug(f"Inserting new doctor: {doctor_uuid}")
                    cur.execute(
                        config.DOCTOR_INSERT_QUERY_SMALL, tuple(doctor_data.values())
                    )
                
                # Insert establishment if not exists
                elif not establishment_exists and "establishment_info" in value:
                    establishment_data = value.get("establishment_info")
                    db_logger.debug(f"Inserting new establishment: {establishment_uuid}")
                    cur.execute(
                        config.ESTABLISHMENT_INSERT_QUERY_SMALL,
                        tuple(establishment_data.values()),
                    )
                
                # Insert the relationship
                db_logger.debug(f"Inserting relationship between {doctor_uuid} and {establishment_uuid}")
                cur.execute(config.RELATIONS_INSERT_QUERY, tuple(relations_data.values()))
                
            except Exception as item_error:
                db_logger.warning(f"Error processing relation item {item_id}: {item_error}")
                # Continue with other items instead of failing the entire batch
                continue
                
        conn.commit()
        db_logger.info(f"Successfully inserted all relation data for {key} with ID {practo_id}")
        
    except Exception as error:
        db_logger.error(f"Error inserting relation data: {error}")
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to insert relation data: {error}", original_error=error)
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()


def insert_main_data(data, query):
    """Insert main entity data into database.

    Args:
        data (dict): Main entity data to insert
        query (str): SQL insert query

    Raises:
        DatabaseError: If database operations fail
    """
    db_logger.info(f"Inserting main data with {len(data)} records")
    
    try:
        conn = connect_db()
        cur = conn.cursor()
        
        # Track success/failure counts
        success_count = 0
        failure_count = 0
        
        for item_id, value in data.items():
            try:
                # Log a sample of the data (first record only)
                if success_count == 0 and failure_count == 0:
                    sample_values = list(value.values())
                    # Truncate sample for logging
                    truncated_sample = [str(v)[:50] + ('...' if len(str(v)) > 50 else '') for v in sample_values[:3]]
                    db_logger.debug(f"Sample data: {truncated_sample}...")
                
                cur.execute(query, tuple(value.values()))
                success_count += 1
                
            except Exception as item_error:
                db_logger.warning(f"Error inserting item {item_id}: {item_error}")
                failure_count += 1
                # Continue with other items instead of failing the entire batch
                continue
        
        conn.commit()
        db_logger.info(f"Main data insertion complete. Success: {success_count}, Failures: {failure_count}")
        
    except Exception as error:
        db_logger.error(f"Error inserting main data: {error}")
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to insert main data: {error}", original_error=error)
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
