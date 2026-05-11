from google.cloud import bigquery
import json

client = bigquery.Client(project='grad2026')

queries = {
    'stg_users': 'SELECT COUNT(*) as c FROM `grad2026.data_production_stg.stg_users__fu`',
    'app_users': 'SELECT COUNT(*) as c FROM `grad2026.data_production_ods.app_users__s2`',
    'app_users_null_pk': 'SELECT COUNT(*) as c FROM `grad2026.data_production_ods.app_users__s2` WHERE pk_user IS NULL',
    'stg_locations': 'SELECT COUNT(*) as c FROM `grad2026.data_staging.locations_ext`',
    'app_locations': 'SELECT COUNT(*) as c FROM `grad2026.data_production_ods.app_locations__snp`',
    'listen_events': 'SELECT COUNT(*) as c FROM `grad2026.data_production_stg.stg_listen_events__fa`'
}

results = {}
for name, q in queries.items():
    try:
        results[name] = list(client.query(q).result())[0].c
    except Exception as e:
        results[name] = str(e)

print(json.dumps(results, indent=2))
