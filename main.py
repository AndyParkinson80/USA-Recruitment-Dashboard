import requests
import math
import json
import os
import pandas as pd
import tempfile

from datetime import datetime, timedelta
from pathlib import Path

from google.auth import default
from google.cloud import bigquery, secretmanager
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account


current_folder = Path(__file__).resolve().parent
data_store = current_folder/"Data - USA"

country = "USA"
Data_export = False
testing = False                                     #True uses local raw data drop, false uses API


def google_auth():
    """
    Authenticate with Google Cloud and return credentials and project ID.

    Order of preference:
    1. Application Default Credentials (Cloud Run / gcloud ADC)
    2. GOOGLE_CLOUD_SECRET environment variable (Codespaces / GitHub secret)
    3. Local service account JSON file (~/.gcp/gcp.json)
    """

    try:
        # 1. Try Application Default Credentials
        credentials, project_id = default()
        print("✅ Authenticated using Application Default Credentials")
        return credentials, project_id

    except DefaultCredentialsError:
        print("⚠️ ADC not available, trying GOOGLE_CLOUD_SECRET env var...")

        # 2. Try service account JSON from env var
        secret_json = os.getenv('GOOGLE_CLOUD_SECRET')
        if secret_json:
            service_account_info = json.loads(secret_json)
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            project_id = service_account_info.get('project_id')
            print("✅ Authenticated using service account from GOOGLE_CLOUD_SECRET")
            return credentials, project_id

        # 3. Try service account JSON from local file
        file_path = os.path.expanduser("~/.gcp/gcp.json")
        if os.path.exists(file_path):
            credentials = service_account.Credentials.from_service_account_file(file_path)
            with open(file_path) as f_json:
                project_id = json.load(f_json).get("project_id")
            print(f"✅ Authenticated using local service account file: {file_path}")
            return credentials, project_id

        raise Exception("❌ No valid authentication method found (ADC, GOOGLE_CLOUD_SECRET, or local file).")

def get_secrets(secret_id):
    def access_secret_version(project_id, secret_id, version_id="latest"):

        client = secretmanager.SecretManagerServiceClient(credentials=credentials)
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")

        return payload

    # Example usage:
    project_id = "api-integrations-412107"
    version_id = "latest" 

    secret = access_secret_version(project_id, secret_id, version_id)
    #print (secret)
    return secret

def load_ssl(certfile_content, keyfile_content):
    """
    Create temporary files for the certificate and keyfile contents.
    
    Args:
        certfile_content (str): The content of the certificate file.
        keyfile_content (str): The content of the key file.
    
    Returns:
        tuple: Paths to the temporary certificate and key files.
    """
    # Create temporary files for certfile and keyfile
    temp_certfile = tempfile.NamedTemporaryFile(delete=False)
    temp_keyfile = tempfile.NamedTemporaryFile(delete=False)

    try:
        # Write the contents into the temporary files
        temp_certfile.write(certfile_content.encode('utf-8'))
        temp_keyfile.write(keyfile_content.encode('utf-8'))
        temp_certfile.close()
        temp_keyfile.close()

        # Return the paths of the temporary files
        return temp_certfile.name, temp_keyfile.name
    except Exception as e:
        # Clean up in case of error
        os.unlink(temp_certfile.name)
        os.unlink(temp_keyfile.name)
        raise e
    
def security(client_id, 
             client_secret, 
             temp_keyfile,
             temp_certfile):
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("        Creating Credentials (" + time_now + ")")

        def adp_bearer():
            adp_token_url = 'https://accounts.adp.com/auth/oauth/v2/token'                                                                                          

            adp_token_data = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret
            }
            adp_headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
            }


            adp_token_response = requests.post(adp_token_url, 
                                                cert=(temp_certfile, temp_keyfile), 
                                                verify=True, 
                                                data=adp_token_data, 
                                                headers=adp_headers)

            if adp_token_response.status_code == 200:
                access_token = adp_token_response.json()['access_token']

            return access_token
    
        access_token = adp_bearer()

        return access_token

def GET_staff_adp():
    current_date = datetime.now()                                      
    months = current_date - timedelta(days=500)
    formatted_date = months.strftime("%Y-%m-%d")

    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Retrieving Current Staff from ADP Workforce Now (" + time_now + ")")
    api_url = 'https://api.adp.com/hr/v2/workers'
    api_headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept':"application/json;masked=false",  
        }
    api_count_params = {
            "count": "true",
        }

    api_count_response = requests.get(api_url, cert=(temp_certfile, temp_keyfile), verify=True, headers=api_headers, params=api_count_params)                 #data request. Find number of records and uses this to find the pages needed
    response_data = api_count_response.json()
    total_number = response_data.get("meta", {}).get("totalNumber", 0)
    rounded_total_number = math.ceil(total_number / 100) * 100

    adp_responses = []                                                                                                                              # Initialize an empty list to store API responses. This will also store the outputted data

    def make_api_request_active(skip_param):                                                                                                        # Function to make an API request with skip_param and append the response to all_responses

        api_headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept':"application/json;masked=false"
            }
 
        api_params = {
            "$top": 100,
            "$skip": skip_param,
            }

        api_response = requests.get(api_url, cert=(temp_certfile, temp_keyfile), verify=True, headers=api_headers, params=api_params)

        if api_response.status_code == 200:
            #checks the response and writes the response to a variable
            json_data = api_response.json()

            # Append the response to all_responses
            adp_responses.append(json_data)

            # Check for a 204 status code and break the loop
            if api_response.status_code == 204:
                return True
        elif api_response.status_code == 204:
            return True
        else:
            print(f"Failed to retrieve data from API for skip_param {skip_param}. Status code: {api_response.status_code}")

    total_records = 0
    skip_param = 0

    while True:
        print (f"           Returning record # {total_records + 1} to {total_records + 100} of {rounded_total_number}")
        make_api_request_active(skip_param)
        skip_param += 100
        total_records += 100 

        if total_records >= rounded_total_number:  
            break

    combined_staff = []
    for item in adp_responses:
        combined_staff.extend(item["workers"])
    
    if Data_export:     
        file_path = os.path.join(data_store,"001a - Raw Staff.json")
        with open(file_path, "w") as outfile:
            json.dump(combined_staff, outfile, indent=4)

    reordered_staff = []
    for staff in combined_staff:
        forename = staff["person"]["legalName"]["givenName"]
        middleName = staff["person"]["legalName"].get("middleName")
        givenName = staff["person"]["legalName"].get("givenName")
        preferredName = (
            None
            if not staff["person"].get("preferredName") 
            else staff["person"]["preferredName"].get("givenName", "")
        )        
        surname = staff["person"]["legalName"]["familyName1"]
        status = staff["workerStatus"]["statusCode"]["codeValue"]
        hireDate = staff["workerDates"]["originalHireDate"]
        address = staff["person"]["legalAddress"]["lineOne"]
        dob = staff["person"]["birthDate"]
        
        position = next(
            (index for index, field in enumerate(staff["workAssignments"]) if field["primaryIndicator"] is True),
        )

        manager = staff["workAssignments"][position].get("reportsTo",None)
        if manager:
            formatted_name = manager[0]["reportsToWorkerName"].get("formattedName", "") 

        transformed_staff = {
            "Forename": forename,
            "MiddleName": middleName,
            "givenName": givenName,
            "prefferedName": preferredName,
            "Surname": surname,
            "Status": status,
            "BirthDate": dob,
            "Address": address,
            "Hire Date": hireDate,
            "Manager": formatted_name
        }
        
        reordered_staff.append(transformed_staff)
    
    filtered_staff = [record for record in reordered_staff if record["Status"] in ["Active", "Inactive"]]
    
    if Data_export:     
        file_path = os.path.join(data_store,"001b - Reordered + Filtered Staff.json")
        with open(file_path, "w") as outfile:
            json.dump(filtered_staff, outfile, indent=4)
    
    return filtered_staff

def GET_applicants_adp(staff):
    if testing is False:
        current_date = datetime.now()                                      
        months = current_date - timedelta(days=500)
        formatted_date = months.strftime("%Y-%m-%d")

        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("        Retrieving Applicants from ADP Workforce Now (" + time_now + ")")
        api_url = 'https://api.adp.com/staffing/v2/job-applications'
        api_headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept':"application/json;masked=false",  
            }
        api_count_params = {
                "count": "true",
            }

        api_count_response = requests.get(api_url, cert=(temp_certfile, temp_keyfile), verify=True, headers=api_headers, params=api_count_params)                 #data request. Find number of records and uses this to find the pages needed
        response_data = api_count_response.json()
        total_number = response_data.get("meta", {}).get("totalNumber", 0)
        rounded_total_number = math.ceil(total_number / 100) * 100

        adp_responses = []                                                                                                                              # Initialize an empty list to store API responses. This will also store the outputted data

        def make_api_request_active(skip_param):                                                                                                        # Function to make an API request with skip_param and append the response to all_responses

            api_headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept':"application/json;masked=false"
                }
    
            api_params = {
                #"$filter": "applicationSource/submittedDate ge 2023-06-01",
                "$top": 20,
                "$skip": skip_param,
                }

            api_response = requests.get(api_url, cert=(temp_certfile, temp_keyfile), verify=True, headers=api_headers, params=api_params)
            #time.sleep(0.6)

            if api_response.status_code == 200:
                #checks the response and writes the response to a variable
                json_data = api_response.json()

                # Append the response to all_responses
                adp_responses.append(json_data)

                # Check for a 204 status code and break the loop
                if api_response.status_code == 204:
                    return True
            elif api_response.status_code == 204:
                return True
            else:
                print(f"Failed to retrieve data from API for skip_param {skip_param}. Status code: {api_response.status_code}")

        total_records = 0
        skip_param = 0

        while True:
            print (f"           Returning record # {total_records + 1} to {total_records + 20} of {rounded_total_number}")
            make_api_request_active(skip_param)
            skip_param += 20
            total_records += 20 

            if total_records >= rounded_total_number:  
                break

        combined_applications = []
        for item in adp_responses:
            combined_applications.extend(item["jobApplications"])
        
        if Data_export:     
            file_path = os.path.join(data_store,"002a - Raw Applications.json")
            with open(file_path, "w") as outfile:
                json.dump(combined_applications, outfile, indent=4)

    if testing:
        print ("Loading data from saved applications")
        file_path = os.path.join(data_store,"002a - Raw Applications.json")
        with open(file_path, "r") as file:
            combined_applications = json.load(file)

    reordered_applications = []
    for apps in combined_applications:
        name = apps["applicant"]["person"]["personName"]["formattedName"]
        forename = apps["applicant"]["person"]["personName"]["givenName"]
        surname = apps["applicant"]["person"]["personName"]["familyName1"]

        app_start = apps["applicationStatusCode"]["effectiveDate"]
        app_dob = apps["applicant"]["person"].get("birthDate")
        app_status = apps["applicationStatusCode"]["shortName"]
        app_job = apps["jobRequisitionReference"].get("requisitionTitle")
        
        hiring_manager = str(apps["jobRequisitionReference"].get("hiringManager", {}).get("personName", {}).get("formattedName"))
        if hiring_manager:
            names = hiring_manager.split(", ")
            if len(names) == 2:
                secondName,firstName = names
                lineManager = f"{firstName} {secondName}"
            else:
                lineManager = ""

        if lineManager == "Zacri Byam":
            lineManager = "Zac Byam"
        
        recruiter = str(apps["jobRequisitionReference"].get("recruiter", {}).get("personName", {}).get("formattedName"))
        requisition_id = apps["jobRequisitionReference"].get("requisitionID")
        address = apps["applicant"]["person"]["address"].get("lineOne","")

        if "Guerrero" in recruiter:
            recruiter = "Robinson Guerrero"
        elif "Dana" in recruiter:
            recruiter = "Dana Schwartz"
        elif "Julia" in recruiter:
            recruiter = "Julia Peoples"
        elif "Robyn" in recruiter:
            recruiter = "Robyn Halliday"
        
        transformed_record = {
            "CandidateName": name,
            "forename": forename,
            "surname": surname,
            "DOB": app_dob,
            "ApplicationStatus": app_status,
            "JobTitle": app_job,
            "HiringManager": hiring_manager,
            "LineManager": lineManager,
            "Recruiter": recruiter,
            "Requisition_ID": requisition_id,
            "Start Date": app_start,
            "Address": address,
            "Match Made": None,
        }
        
        reordered_applications.append(transformed_record)

    for app in reordered_applications:                          #Tries to find a matching staff member in the ADP record
        app_forename = app.get("forename", "").lower()
        app_surname = app.get("surname","").lower()
        app_start_date = app.get("Start Date")
        app_manager = app.get("Manager")
        app_dob = app.get("DOB","")

        # Flag to check if a matching record is found
        match_found = False
        
        for record in staff:
            staff_forename = record.get("Forename", "").lower()
            staff_middlename = record.get("middleName","").lower()
            staff_given_name = record.get("givenName","").lower
            staff_preferred_name = record.get("preferredName","").lower
            staff_surname = record.get("Surname", "").lower()
            staff_status = record.get("Status", "").lower()
            staff_hire_date = record.get("Hire Date")
            staff_lineManager = record.get("LineManager")
            staff_dob = record.get("BirthDate")

            # Match criteria
            matches = 0
            if app_forename in {staff_forename, staff_middlename, staff_given_name,staff_preferred_name}:
                matches += 1
            if app_surname == staff_surname:
                matches += 1
            if app_manager == staff_lineManager:
                matches += 1
            
            if app_start_date and staff_hire_date:
                start_date = datetime.strptime(app_start_date, "%Y-%m-%d")
                hire_date = datetime.strptime(staff_hire_date, "%Y-%m-%d")
                if abs((start_date - hire_date).days) <= 5:
                    matches += 1
            
            if app_dob == staff_dob:
                matches +=1

            # Check if two or more criteria match
            if matches >= 3 and ("active" in staff_status or "inactive" in staff_status):
                match_found = True
                break
        
        # Set the 'Active?' field based on match
        if match_found:
            app["Match Made"] = True

    if Data_export:     
        file_path = os.path.join(data_store,"002b - New Applications.json")
        with open(file_path, "w") as outfile:
            json.dump(reordered_applications, outfile, indent=4)
    

    keywords_to_include = ["Offer","Screening","Hire"]
    keywords_to_exclude = ["Deleted","Declined"]

    filtered_applications = [
        application for application in reordered_applications
        if any(keyword in application["ApplicationStatus"] for keyword in keywords_to_include)
        and not any(keyword in application["ApplicationStatus"] for keyword in keywords_to_exclude)
    ]

    sorted_applications = sorted(filtered_applications, key=lambda x: (x["CandidateName"], x["ApplicationStatus"]))

    filtered_applications = {}
    for application in sorted_applications:
        candidate_name = application["CandidateName"]
        if (
            candidate_name not in filtered_applications or
            application["ApplicationStatus"] == "Hired"
        ):
            filtered_applications[candidate_name] = application
    
    filtered_applications = list(filtered_applications.values())


    if Data_export:     
        file_path = os.path.join(data_store,"002c - Filtered Applications.json")
        with open(file_path, "w") as outfile:
            json.dump(filtered_applications, outfile, indent=4)

    return filtered_applications

def GET_reqs():
    #if testing is False:
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Retrieving Requisitions from ADP Workforce Now (" + time_now + ")")
    api_url = 'https://api.adp.com/staffing/v1/job-requisitions'
    api_headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept':"application/json;masked=false",  
        }

    api_count_params = {
            "count": "true",
        }

    api_count_response = requests.get(api_url, cert=(temp_certfile, temp_keyfile), verify=True, headers=api_headers, params=api_count_params)                 #data request. Find number of records and uses this to find the pages needed
    response_data = api_count_response.json()
    total_number = response_data.get("meta", {}).get("totalNumber", 0)
    rounded_total_number = math.ceil(total_number / 100) * 100

    adp_responses = []                                                                                                                              # Initialize an empty list to store API responses. This will also store the outputted data

    def make_api_request_active(skip_param):                                                                                                        # Function to make an API request with skip_param and append the response to all_responses

        api_headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept':"application/json;masked=false"
            }
        api_params = {
            "$top": 20,
            "$skip": skip_param,
            }

        api_response = requests.get(api_url, cert=(temp_certfile, temp_keyfile), verify=True, headers=api_headers, params=api_params)
        #time.sleep(0.6)

        if api_response.status_code == 200:
            #checks the response and writes the response to a variable
            json_data = api_response.json()

            # Append the response to all_responses
            adp_responses.append(json_data)

            # Check for a 204 status code and break the loop
            if api_response.status_code == 204:
                return True
        elif api_response.status_code == 204:
            return True
        else:
            print(f"Failed to retrieve data from API for skip_param {skip_param}. Status code: {api_response.status_code}")

    total_records = 0
    skip_param = 0

    while True:
        print (f"Returning record # {total_records + 1} to {total_records + 20} of {rounded_total_number}")
        make_api_request_active(skip_param)
        skip_param += 20
        total_records += 20 

        if total_records >= rounded_total_number:  
            break

    combined_requisitions = []
    for item in adp_responses:
        combined_requisitions.extend(item["jobRequisitions"])

    if Data_export:     
        file_path = os.path.join(data_store,"003a -Raw Requisitions.json")
        with open(file_path, "w") as outfile:
            json.dump(combined_requisitions, outfile, indent=4)


    reordered_requisitions = []
    for reqs in combined_requisitions:
        req_id = reqs["itemID"]
        postdate = reqs["postingInstructions"][0].get("postDate")
        backfill = reqs.get("backfillWorkerPositions")
        new = reqs.get("openingsNewPositionQuantity")

        if backfill:
            req_type = "Backfill"
        elif new:
            req_type = "New Role"
        else:
            req_type = None

        transformed_record = {
            "Requisition ID": req_id,
            "Posted Date": postdate,
            "req_type": req_type
        }
        
        reordered_requisitions.append(transformed_record)

    if Data_export:     
        file_path = os.path.join(data_store,"003 - Requisitions.json")
        with open(file_path, "w") as outfile:
            json.dump(reordered_requisitions, outfile, indent=4)


    return reordered_requisitions

def filter_adp(adp_applications,adp_reqs):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Creating data Table (" + time_now + ")")
    
    output = []
    
    for apps in adp_applications:
        name = apps["CandidateName"] 
        status = apps["ApplicationStatus"]     
        jobTitle = apps["JobTitle"]     
        hiringManager = apps["HiringManager"]     
        recruiter = apps["Recruiter"]     
        recID = apps["Requisition_ID"]     
        hireDate = apps["Start Date"]
        onRoll = apps["Match Made"]     
    
        posted_date = None
        for req in adp_reqs:
            if req["Requisition ID"] == recID:
                posted_date = req.get("Posted Date") 
                req_type = req.get("req_type") 
                break  

        if posted_date:
            posted_date = posted_date[:10]
            start_date_dt = datetime.strptime(hireDate, "%Y-%m-%d")
            posted_date_dt = datetime.strptime(posted_date, "%Y-%m-%d")
            days_to_hire = max(0, (start_date_dt - posted_date_dt).days)
        else:
            days_to_hire = 0

        if hireDate:
            hire_date_parsed = datetime.strptime(hireDate, "%Y-%m-%d")  
            if hire_date_parsed >= datetime.now() - timedelta(days=21):         #check this with Stephanie            
                onRoll = True

        transformed_record = {
            "CandidateName": name,
            "ApplicationStatus": status,
            "JobTitle": jobTitle,
            "HiringManager": hiringManager,
            "Recruiter": recruiter,
            "RequisitionCreateDate": posted_date,
            "DateofHire": hireDate,
            "DaystoHire": days_to_hire,
            "StillEmployed": onRoll,
            "ReqType": req_type,
        }
        


        output.append(transformed_record)

    if Data_export:
        file_path = os.path.join(data_store, "004 - Export to looker.json")
        with open(file_path, "w") as outfile:
            json.dump(output, outfile, indent=4)

        file_path = os.path.join(data_store, "005 - main schema.csv")
        df=pd.DataFrame(output)
        df.to_csv(file_path, index=False)
    
    return output

def reload_bigquery():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Rebuilding Data Table in bigquery (" + time_now + ")")

    client = bigquery.Client(credentials=credentials, project=project)

    project_id = "api-integrations-412107"
    dataset_id = "usa_recruitment_dashboard"
    table_id = "main"
            
    def delete_table_data(project_id, dataset_id, table_id):
        query = f"DELETE FROM `{project_id}.{dataset_id}.{table_id}` WHERE TRUE"
        client.query(query).result()  # Executes the query
        print(f"All rows deleted from {table_id}")

    def load_data(data,project_id, dataset_id, table_id):
        df = pd.DataFrame(data)

        df["RequisitionCreateDate"] = pd.to_datetime(df["RequisitionCreateDate"], errors='coerce')
        df["DateofHire"] = pd.to_datetime(df["DateofHire"], errors='coerce')

        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        job = client.load_table_from_dataframe(df, table_ref)  # Load data
        job.result()  # Wait for the job to complete
        print(f"Data loaded into {table_id}")

    
    delete_table_data(project_id, dataset_id, table_id)
    load_data(looker_data,project_id, dataset_id,table_id)

if __name__ == "__main__":
    credentials, project = google_auth()

    client_id = get_secrets("ADP-usa-client-id")
    client_secret = get_secrets("ADP-usa-client-secret")
    keyfile = get_secrets("usa_cert_key")
    certfile = get_secrets("usa_cert_pem")

    temp_certfile, temp_keyfile = load_ssl(certfile, keyfile)

    access_token  = security(client_id, client_secret,temp_keyfile,temp_certfile)

    current_staff                                                                                           = GET_staff_adp()
    adp_applications                                                                                        = GET_applicants_adp(current_staff)

    if testing is False:
        adp_reqs                                                                                                = GET_reqs()
    if testing:
        print ("Loading data from saved requisitions")
        file_path = os.path.join(data_store,"003 - Requisitions.json")
        with open(file_path, "r") as file:
            adp_reqs = json.load(file)

    looker_data                                                                                             = filter_adp(adp_applications,adp_reqs)
    reload_bigquery()

    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Finishing Up (" + time_now + ")")

