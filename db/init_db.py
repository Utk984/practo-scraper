import os
import psycopg2
from config import DATABASE_URL

def init_db():
    """Initialize the database by executing mapping.sql if tables don't exist."""
    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        # Check if tables exist
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('doctors', 'establishments', 'doctor_establishment')
        """)
        table_count = cursor.fetchone()[0]

        # If not all tables exist, execute the mapping.sql
        if table_count < 3:  # We expect 3 tables
            print("Initializing database tables...")
            # Get the absolute path to mapping.sql
            current_dir = os.path.dirname(os.path.abspath(__file__))
            mapping_sql_path = os.path.join(current_dir, 'mapping.sql')
            
            # Read and execute the SQL commands from mapping.sql
            with open(mapping_sql_path, 'r') as sql_file:
                sql_script = sql_file.read()
                cursor.execute(sql_script)
                conn.commit()
            print("Database tables created successfully!")
        else:
            print("Database tables already exist.")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Error initializing database: {e}")
        raise