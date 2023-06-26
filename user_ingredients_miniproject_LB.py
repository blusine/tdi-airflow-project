import os
import glob
import json
import requests
import time
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
import hashlib


from airflow.models.dag import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python_operator import PythonOperator
#from airflow.hooks.postgres_hook import PostgresHook
#from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
#from airflow.hooks.base_hook import BaseHook

#This project assumes an s3 bucket exists with the name provided by bucket_name variable
#The project also asumes two foldernames are defined within the bucket, and the folder names are provided by the variables raw_folder_name and stage_folder_name
bucket_name = 'de-airflow-miniproject'
raw_folder_name = 'raw/'
stage_folder_name = 'stage/'
#define the number of submissions you want to put in one file
len_submissions = 5

def fn_request_user_submissions():
    # Collect user ingredient submissions
    submissions = []
    while len(submissions) < len_submissions:
        response = requests.get('https://airflow-miniproject.onrender.com/')
        time.sleep(2)
        r = response.json()
        submissions.append(json.dumps(r))
        #deduplicate submissions before return
        submissions = list(set(submissions))
    return submissions

def fn_write_raw_data_to_s3(user_filename, **kwargs):
    # Write deduplicated submissions to S3
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ti = kwargs['ti']
    submissions = ti.xcom_pull(task_ids='request_user_submissions_task')
    s3_raw_filename = f'{raw_folder_name}{user_filename}'
    s3_hook.load_string(json.dumps(submissions), key=s3_raw_filename, bucket_name=bucket_name)
    return s3_raw_filename
  
    
def fn_process_s3_raw_file(s3_raw_filename, stage_filename, **context):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    #even though S3KeySensor tells us the filekey is in s3 bucket, the file may not be fully uploaded, we wait a little to give time for upload. We will not be able to read the file if it is not uploaded. Because the files are not big, I hope sleeptime would help us. In reality, probably we would check if the actual upload was completed.
    time.sleep(10)
    all_recipes = []
    file_content = s3_hook.read_key(s3_raw_filename, bucket_name=bucket_name)
    file_content = json.loads(file_content.strip(), strict=False)
    submissions = []
    #submission is one of the 20 submissions by a user that we saved in s3_raw_filename - raw/ folder, and are now reading from
    for submission in file_content:
        submission = json.loads(submission.strip(), strict=False)
        
        #create a unique userid
        string = submission['user']['address'].replace('\n', ',') + "_" + submission['user']['name']
        string = string.replace(" ", "")
        hash_object = hashlib.sha256(string.encode())
        submission['user']['user_id'] = hash_object.hexdigest()

        #create a unique pantryid
        string = str(submission['pantry']['excludeIngredients']) + "_" + str(submission['pantry']['includeIngredients']) + "_" + str(submission['pantry']['intolerances'])
        string = string.replace(" ", "")
        hash_object = hashlib.sha256(string.encode())
        submission['pantry']['pantry_id'] = hash_object.hexdigest()

        #request spoonacular api for the submission
        time.sleep(2)
        base = 'https://api.spoonacular.com/recipes/complexSearch'
        params = {
            'apiKey': 'spoonacular_api_key',
            'addRecipeInformation': True,
            'includeIngredients': submission["pantry"]["includeIngredients"],
            'excludeIngredients': submission["pantry"]["excludeIngredients"],
            "intolerances": submission["pantry"]["intolerances"]
        }
        response = requests.get(base, params)
        if response.status_code == 200:
            raw_recipes = response.json()
            #raw_recipes is the list of recipes extracted from spoonaculare for 1 user submission
            raw_recipes = raw_recipes['results']
            #user_recipes will hold the information we need for each user, created from raw_recipes and submission
            user_recipes = []
            if len(raw_recipes) != 0:
                for item in raw_recipes:
                    single_recipe = {}
                    single_recipe["recipe_id"] = item["id"]
                    single_recipe["title"] = item["title"]
                    single_recipe["servings"] = item["servings"]
                    single_recipe['summary'] = item["summary"]
                    single_recipe['diets'] = item["diets"]
                    #single_recipe['ingredients'] = item["servings"]
                    #single_recipe['equipment'] = item["servings"]
                    ingredients = []
                    equipments = []
                    
                    #Check if analyzedInstructions are in the recipe
                    if item['analyzedInstructions'] and len(item['analyzedInstructions']) > 0:    
                        #Check if steps are defined in analyzedInstructions
                        if item['analyzedInstructions'][0]['steps'] and len(item['analyzedInstructions'][0]['steps']) > 0:
                            #print("analyzedInstructions[0]['steps'] type: ", type(item['analyzedInstructions'][0]['steps']))
                            #print("analyzedInstructions[0]['steps']: ", item['analyzedInstructions'][0]['steps'])
                            #Iterate over steps to pull out ingredients and equipment for the recipe
                            for step in item['analyzedInstructions'][0]['steps']:
                                #print(" type of step: ", type(step))
                                #print("step: ", step)
                                #check if ingredients are listed in the step
                                if step['ingredients'] and len(step['ingredients']) > 0:
                                    #print("step['ingredients'] type: ", type(step['ingredients']))
                                    #print("step['ingredients'] : ", step['ingredients'])
                                    
                                    for ing in step['ingredients']:
                                        ingredients.append(ing['name'])
                                    ingredients = list(set(ingredients))
                                
                                #check if equipment are listed in the step
                                if step['equipment'] and len(step['equipment']) > 0:
                                    #print("step['equipment'] type: ", type(step['equipment']))
                                    #print("step['equipment'] : ", step['equipment'])
                                    
                                    for eq in step['equipment']:
                                        equipments.append(eq['name'])
                                    equipments = list(set(equipments))
                            #print("ingredients: ", ingredients)
                            #print("equipments: ", equipments)
                            
                    single_recipe['ingredients'] = ingredients
                    single_recipe['equipment'] = equipments
                    #print("single_recipe: ", single_recipe)
                    user_recipes.append(single_recipe)
                
            #print("user_recipes: ", user_recipes)
            submission['recipes'] = user_recipes
            #print("submission: ", submission)
            submissions.append(submission)
            
        else:
            print("Status Code: ", response.status_code)

        #print(submission["pantry"]["includeIngredients"])
        #print("type of pantry", type(submission["pantry"]))
        #print(submission["user"])
        #print("type of user", type(submission["user"]))

    #I may be saving too much info, probably I could have optimized this but for now, I am saving into the staging folder everything I have. In the next task, I will use this information to create multiple tables for the postgres database.
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_stage_filename = f'{stage_folder_name}{stage_filename}'
    s3_hook.load_string(json.dumps(submissions), key=s3_stage_filename, bucket_name=bucket_name)
    return s3_stage_filename

def util_fn_create_db_tables():
    """This function creates database tables. It can be called only the first time when the database is empty."""

    #connect to postgres database and create tables if they don't exist yet
    #pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    #pg_hook = BaseHook.get_hook(conn_id='postgres_default')
    #conn = pg_hook.get_conn()
    #I was not able to connect through PostgresHook or BaseHook, so I had to put my database details directly here.
    
    #create db connection
    conn = psycopg2.connect(
    host='host_name',
    port='port_number',
    dbname='db_name',
    user='user_name',
    password='password'
    )
    cursor = conn.cursor()
        
    #create the tables
    create_user_table_query = """
           create table IF NOT EXISTS "user" (user_id varchar(64) primary key, name varchar, address varchar, description varchar);
        """
    cursor.execute(create_user_table_query)
    print("user table created.")

    create_pantry_table_query = """
           create table IF NOT EXISTS "pantry" (pantry_id varchar(64) primary key, excludeIngredients varchar, includeIngredients varchar, intolerances varchar);
        """
    cursor.execute(create_pantry_table_query)
    print("pantry table created.")

    #here I am assuming recipe_id is the primary key of the recipe in the spoonacular database. It is very unsafe assumption, in reality I sould have created my own primary key. For now, I am keeping my assumption and hoping this code does not break.
    create_recipes_table_query = """
           create table IF NOT EXISTS "recipes" (
           recipe_id int primary key, 
           title varchar, 
           servings int, 
           summary varchar,
           diets varchar,
           ingredients varchar,
           equipment varchar
           );
        """
    cursor.execute(create_recipes_table_query)
    print("recipes table created.")

    #I think user_recipes is not required as long as pantry_user and pantry_recipes tables are available. However, I am creating this as I thought that was one of the miniproject's requirements.
    create_user_recipes_table_query = """
           create table IF NOT EXISTS "user_recipes" (
            user_id varchar(64),
            recipe_id bigint,
            FOREIGN KEY (user_id) REFERENCES "user" (user_id),
            FOREIGN KEY (recipe_id) REFERENCES "recipes" (recipe_id)
           
           );
        """
    cursor.execute(create_user_recipes_table_query)
    print("user_recipes table created.")

    create_pantry_recipes_table_query = """
           create table IF NOT EXISTS "pantry_recipes" (
            pantry_id varchar(64),
            recipe_id bigint,
            FOREIGN KEY (pantry_id) REFERENCES "pantry" (pantry_id),
            FOREIGN KEY (recipe_id) REFERENCES "recipes" (recipe_id)
           );
        """
    cursor.execute(create_pantry_recipes_table_query)
    print("pantry_recipes table created.")


    create_pantry_users_table_query = """
           create table IF NOT EXISTS "pantry_user" (
            pantry_id varchar(64),
            user_id varchar(64),
            FOREIGN KEY (pantry_id) REFERENCES "pantry" (pantry_id),
            FOREIGN KEY (user_id) REFERENCES "user" (user_id)
           );
        """
    cursor.execute(create_pantry_users_table_query)
    print("pantry_user table created.")
    
    conn.commit()
    cursor.close()
    conn.close()
        

def fn_process_s3_stage_file(s3_stage_filename, **context):
    """This function reads from s3 stage folder and updates the postgres database"""
    #connect to s3 and read staged data
    s3_hook = S3Hook(aws_conn_id='aws_default')
    time.sleep(10)
    file_content = s3_hook.read_key(s3_stage_filename, bucket_name=bucket_name)
    file_content = json.loads(file_content.strip(), strict=False)
    #print("type of file_content[0]: ", type(file_content[0]))
    #print("file_content: ", file_content)
    
    #call the create tables function. Comment out if the tables exist.
    util_fn_create_db_tables()

    #create db connection
    conn = psycopg2.connect(
    host='host_name',
    port='port_number',
    dbname='db_name',
    user='user_name',
    password='password'
    )
    cursor = conn.cursor()
    
    for submission in file_content:
        #submission = json.loads(submission)
        print("type of submission[recipes]: ", type(submission["recipes"]))
        print("length of submission[recipes]: ", len(submission["recipes"]))
        
        # Insert data into the user table
        insert_user_query =  sql.SQL("""
        INSERT INTO "user" (address, description, name, user_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """)
        cursor.execute(insert_user_query, tuple(submission["user"].values()))
        
        # Insert data into the pantry table
        insert_pantry_query =  sql.SQL("""
        INSERT INTO "pantry" (excludeIngredients, includeIngredients, intolerances, pantry_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """)
        cursor.execute(insert_pantry_query, tuple(submission["pantry"].values()))
        
        # Insert data into the pantry_user table
        insert_user_pantry_query =  sql.SQL("""
        INSERT INTO "pantry_user" (user_id, pantry_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """)
        cursor.execute(insert_user_pantry_query, (submission["user"]["user_id"], submission["pantry"]["pantry_id"]))
        
        #iterate over recipes to insert in database if the list of recipes is not empty
        if len(submission["recipes"]) > 0:
            for recipe in submission["recipes"]:

                #insert into the recipes table
                insert_recipe_query =  sql.SQL("""
                INSERT INTO "recipes" (recipe_id, title, servings, summary, diets, ingredients, equipment)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """)
                cursor.execute(insert_recipe_query, (
                    recipe["recipe_id"], 
                    recipe["title"], 
                    recipe["servings"], 
                    recipe["summary"], 
                    recipe["diets"], 
                    recipe["ingredients"], 
                    recipe["equipment"]))

                #insert into the user_recipes table
                insert_user_recipe_query =  sql.SQL("""
                INSERT INTO "user_recipes" (user_id, recipe_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """)
                cursor.execute(insert_user_recipe_query, (
                    submission["user"]["user_id"],
                    recipe["recipe_id"] ))
                
                #insert into the pantry_recipes table
                insert_pantry_recipe_query =  sql.SQL("""
                INSERT INTO "pantry_recipes" (pantry_id, recipe_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """)
                cursor.execute(insert_pantry_recipe_query, (
                    submission["pantry"]["pantry_id"],
                    recipe["recipe_id"]
                    ))
                                
                
        #Insert data into the recipes table
        #insert_query = sql.SQL("INSERT INTO recipes (recipe_id, title, servings, summary, diets, ingredients, equipment) VALUES {} ON CONFLICT DO NOTHING").format(
        #   sql.SQL(',').join(map(sql.Literal, list(submission["recipes"])))
        #)
        
        print("recipes inserted")

        #
        #for recipe in submission["recipes"]:
        
        
    conn.commit()
    cursor.close()
    conn.close()
        
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=1),
    'depends_on_past': False,
    'catchup': False # default
}

with DAG(
    dag_id = 'user_ingredients_dag', 
    default_args = default_args, 
    schedule_interval = '* * * * *'
) as dag:
    
    request_user_submissions_task = PythonOperator(
        task_id = 'request_user_submissions_task',
        python_callable = fn_request_user_submissions,
        provide_context=True,
        dag=dag,
    )

    upload_raw_data_task = PythonOperator(
        task_id='upload_raw_data_task',
        python_callable=fn_write_raw_data_to_s3,
        op_kwargs = {'user_filename': 'user_submit-{{ ts }}.json'},
        provide_context=True,
        dag=dag,
    )

    wait_for_raw_file_sensor_task = S3KeySensor(
        task_id='wait_for_raw_file_sensor_task',
        bucket_name=bucket_name,        
        bucket_key= '{{ ti.xcom_pull(task_ids="upload_raw_data_task") }}',
        timeout=180,
        poke_interval=30,
        dag=dag
    )

    process_raw_data_task = PythonOperator(
        task_id='process_raw_data_task',
        python_callable=fn_process_s3_raw_file,
        op_kwargs = {'s3_raw_filename': '{{ ti.xcom_pull(task_ids="upload_raw_data_task") }}', 'stage_filename': 'staged_data-{{ ts }}.json'},
        provide_context=True,
    )

    wait_for_stage_file_sensor_task = S3KeySensor(
        task_id='wait_for_stage_file_sensor_task',
        bucket_name=bucket_name,        
        bucket_key= '{{ ti.xcom_pull(task_ids="process_raw_data_task") }}',
        timeout=180,
        poke_interval=30,
        dag=dag
    )

    process_stage_data_to_postgres_task = PythonOperator(
        task_id='process_stage_data_to_postgres_task',
        python_callable=fn_process_s3_stage_file,
        op_kwargs = {'s3_stage_filename': '{{ ti.xcom_pull(task_ids="process_raw_data_task") }}'},
        provide_context=True,
    )
    request_user_submissions_task >> upload_raw_data_task >> wait_for_raw_file_sensor_task >> process_raw_data_task >> wait_for_stage_file_sensor_task >> process_stage_data_to_postgres_task

