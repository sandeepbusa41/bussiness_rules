def get_data_sources(business_rules_db, case_id, column_name, master_data_columns=None, master=False):
    """Helper to get all the required table data for the business rules to apply"""
    
    if master_data_columns is None:
        master_data_columns = {}

    def fetch_data(db, table, case_id, columns_list=None):
        query = f"SELECT {columns_list or '*'} from {table}"
        params = [case_id] if not master else []
        df = db.execute_(query, params=params)
        return df.to_dict(orient='records') if not df.empty else {}

    # Retrieve data sources
    data_sources = business_rules_db.execute_("SELECT * from data_sources")
    sources = json.loads(list(data_sources[column_name])[0]) if data_sources[column_name] else {}

    logging.info(f"sources: {sources}")
    if not sources:
        return {}, sources  # Early return if no sources

    data = {}
    # Process data sources
    for database, tables in sources.items():
        db = DB(database, **db_config)
        for table in tables:
            columns_list = ', '.join(master_data_columns.get(table, ['*'])) if master else None
            data[table] = fetch_data(db, table, case_id, columns_list)

    return data, sources
