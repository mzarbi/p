def execute_query(self, query):
    if 'column' in query and 'value' in query:
        # Base case: simple rule
        column = query['column']
        value = query['value']
        matching_files = set()
        for (file_path, column_name), filter_data in self.filters.items():
            if column_name == column:
                if filter_data['type'] == 'bloom' and value in filter_data['filter']:
                    matching_files.add(file_path)
                elif filter_data['type'] == 'range' and filter_data['min'] <= value <= filter_data['max']:
                    matching_files.add(file_path)
        return matching_files
    else:
        # Recursive case: AND/OR condition
        condition = query['condition']
        rules = query['rules']
        matching_files = self.execute_query(rules[0]) if rules else set()
        for rule in rules[1:]:
            rule_files = self.execute_query(rule)
            if condition == 'AND':
                matching_files.intersection_update(rule_files)
            elif condition == 'OR':
                matching_files.update(rule_files)
        return matching_files