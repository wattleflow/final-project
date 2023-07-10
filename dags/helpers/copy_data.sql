COPY staging_{table_name} 
    FROM '{csv_file_path}'
    ACCESS_KEY_ID '{self.access_key}'
    SECRET_ACCESS_KEY '{self.secret_key}'
    WITH (FORMAT CSV, HEADER True);