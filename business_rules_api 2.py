def log_and_return(message, flag=False):
    logging.info(message)
    return jsonify({"flag": flag, "message": message})

def process_time_and_memory(start_time, memory_before):
    try:
        memory_after = measure_memory_usage()
        memory_consumed = (memory_after - memory_before) / (1024 * 1024 * 1024)
        end_time = tt()
        logging.info(f"checkpoint memory_after - {memory_after}, memory_consumed - {memory_consumed:.10f}, end_time - {end_time}")
        return f"{memory_consumed:.10f}", str(round(end_time - start_time, 3))
    except Exception:
        logging.warning("Failed to calculate RAM and time at the end")
        return None, None

def fetch_rule_data(rule_id, business_db):
    fetch_query = f"SELECT * FROM rule_base WHERE rule_id = '{rule_id}'"
    rule_dict = business_db.execute(fetch_query).to_dict(orient="records")
    if not rule_dict:
        return {"flag": True, "data": {}}
    
    rule_data = process_rule_data(rule_dict[0])
    logging.info(f"Fetched rule for {rule_id}: {rule_data}")
    return {"flag": True, "data": rule_data}

def process_rule_data(rule_data):
    return {
        **rule_data,
        'rule': {
            'xml': rule_data.pop('xml'),
            'javascript': rule_data.pop('javascript_code'),
            'python': rule_data.pop('python_code')
        }
    }

def handle_rule(flag, rule_data, business_db, rule_id):
    if flag == 'save':
        return save_rule(rule_data, business_db, rule_id)
    if flag == 'edit':
        return edit_rule(rule_data, business_db, rule_id)
    return None

def save_rule(rule_data, business_db, rule_id):
    rule_data['rule_id'] = rule_id
    if not business_db.insert_dict(table="rule_base", data=rule_data):
        return log_and_return("Duplicate Rule ID or Error saving the rule to DB")
    return None

def edit_rule(rule_data, business_db, rule_id):
    business_db.update(table="rule_base", update=rule_data, where={"rule_id": rule_id})
    return None

def validate_input(data):
    tenant_id = data.get('tenant_id')
    rule_id = data.get('rule_id', "")
    if not tenant_id or not rule_id:
        return log_and_return("Please send valid request data"), None
    return tenant_id, rule_id

def validate_user_and_flag(username, flag):
    if not username or not flag:
        return log_and_return("Invalid user or flag")
    return True

def execute_rule(data):
    string_python = data.get('rule', {}).get('python', "")
    return_param = data.get('return_param', "return_data")
    return test_business_rule(string_python, return_param)

def rule_builder_data():
    memory_before, start_time = initialize_timing_and_memory()
    data = request.json

    # Validate input
    error_response = validate_input(data)
    if error_response:
        return error_response

    tenant_id, rule_id = validate_input(data)
    trace_id = data.get('case_id') or rule_id
    attr = ZipkinAttrs(trace_id=trace_id, span_id=generate_random_64bit_string(), parent_span_id=None, flags=None, is_sampled=False, tenant_id=tenant_id)

    with zipkin_span(service_name='business_rules_api', span_name='rule_builder_data', transport_handler=http_transport, zipkin_attrs=attr, port=5010, sample_rate=0.5):
        username = data.get('user', {}).get('username', "")
        flag = data.get('flag', "")
        rule_name = data.get('rule_name', "")
        
        # Validate user and flag
        error_response = validate_user_and_flag(username, flag)
        if error_response is not True:
            return error_response

        rule_data = {
            'rule_name': rule_name,
            'description': data.get('description', ""),
            'xml': data.get('rule', {}).get('xml', ""),
            'python_code': data.get('rule', {}).get('python', ""),
            'javascript_code': data.get('rule', {}).get('javascript', ""),
            'last_modified_by': username
        }

        db_config['tenant_id'] = tenant_id
        business_db = DB("business_rules", **db_config)

        # Handle rule based on flag
        if flag in ['save', 'edit']:
            result = handle_rule(flag, rule_data, business_db, rule_id)
            if result:
                return result
        elif flag == 'fetch':
            return jsonify(fetch_rule_data(rule_id, business_db))
        elif flag == 'execute':
            return execute_rule(data)

    memory_consumed, time_consumed = process_time_and_memory(start_time, memory_before)
    logging.info(f"BR Time and RAM checkpoint: Time consumed: {time_consumed}, RAM consumed: {memory_consumed}")

    return jsonify(return_data)

def initialize_timing_and_memory():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
        return memory_before, start_time
    except Exception:
        logging.warning("Failed to start RAM and time calculation")
        return None, None
