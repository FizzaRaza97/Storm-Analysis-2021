from google.cloud import bigquery

def query_stackoverflow():
    client = bigquery.Client()
    query_job = client.query(
        """
        SELECT
          CONCAT(
            'https://stackoverflow.com/questions/',
            CAST(id as STRING)) as url,
          view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE tags like '%google-bigquery%'
        ORDER BY view_count DESC
        LIMIT 10"""
    )

    results = query_job.result()  # Waits for job to complete.

    for row in results:
        print("{} : {} views".format(row.url, row.view_count))

def save_tables(project,dataset_id):

    import os

    if not os.path.exists("../data/{}/".format(dataset_id)):
        os.mkdir("../data/{}/".format(dataset_id))

    client = bigquery.Client()
    
    dataset_ref = bigquery.DatasetReference(project,dataset_id)
    
    tables = client.list_tables(project+"."+dataset_id)  # Make an API request.
    
    
    for table in list(tables)[-23:]:
        table_ref = dataset_ref.table(table.table_id)

        df = client.list_rows(client.get_table(table_ref)).to_dataframe()

        df.to_csv("../data/{}/{}.csv".format(dataset_id,table.table_id))

        print("{} exported".format(table.table_id))






if __name__ == "__main__":
    project = "bigquery-public-data"
    dataset_id = "noaa_historic_severe_storms"
    save_tables(project, dataset_id)