# project setup 

1. create a venv in the root of the project 
   python -m venv venv
2. install the package (all the package and their version are mentioned)
   pip install -r truereadapi\lamdareq.txt
3. to run the project go inside the project 
   cd .\truereadapi\
4. run the project
   python manage.py runserver

# This project is hosted in lambda through aws sam.
The image will be built locally and pushed to ecr . to build the image you need to have to docker open in background

1. to build sam in local
   sam build
2. test the image locally 
   sam local start-api   <!-- to run on the local systen -->
   sam local start-api --host 0.0.0.0 --port 8000  <!-- to run over network -->
3. push to lambda(this command will pushh the image to ECR). This uses api creditails in the local system . so make sure to change samconfig.toml  with profile variable to system profile that has permission to push the image to ECR.
   sam deploy