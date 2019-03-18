import json


# Parse the JSON response
with open('json_example') as f:
    data = json.load(f)
    result_set = data["result"]

    print(result_set)

    # Find all authors of each UID
    for element in result_set:
        if str(element) == str(result_set['uids'][0]):
            df_date = result_set[element]['pubdate']
            for author_elem in result_set[element]['authors']:
                df_author_name = author_elem['name']