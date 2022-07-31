# InvestorBot Stock Analysis Tool

# Steps to Use the Stock Analysis Tool

Clone this repository to your machine using the command git clone https://github.com/ezramuskat/InvestorBot.git
Install the necessary libraries on your machine using the command pip3 install -r requirements.txt
Set up a MySQL database (Using Amazon RDS is recommended - see steps listed below).
Create an account at SEC-API to obtain an API Key.
Input the relevant database information and query API where listed below and run the program using python3.
Follow the instructions displayed in the command line.
View the files that have been created in your working directory.

## 1. Clone Repo

Open the command line interface on your local machine
Clone the repo with the command: 
git clone https://github.com/ezramuskat/InvestorBot.git
## 2. Installing Required Python Libraries

This application is intended for use with Python 3.x.
Install the necessary dependencies on your machine with the following commands:
python3 -m pip install -r requirements.txt
## 3. Setting up Amazon Web Services RDS

From the AWS Management console open EC2:
If you would like to create a new security group, do the following steps; otherwise, skip to "Under 'Inbound Rules'". Click the orange "Create security group" button.
If your default secutity Choose a "Security group name" and "Description".
Save the security group name.
Under "Inbound rules" select "Add rule".
Under "Type" select "All traffic" and under "Source" select "Anywhere-IPv4".
Select "Add rule" again.
Under "Type" select "All traffic" and under "Source" select "Anywhere-IPv6".
From the AWS Management console open RDS.
Click the orange "Create database" button.
Under "Engine options" select MySQL.
Under "Templates" select "Free tier".
Under "Settings" choose a name for your Database instance.
Note that this is not the name of the database.
Under "Credentials settings" choose a "Master username" and "Master password" and confirm the password.
Save the username and password.
Under "DB instance class" select "db.t2.micro".
Under "Public access" select "Yes".
Under "VPC Security group" select "Choose existing" and under "Existing security groups" choose the security group you created.
Under "Database authentication" select "Password authentication"
Under "Additional configuration" choose an "Initial databse name" for your database.
Save this name.
If this is not done, the database will not be created.
Click "Create database".
Once the database is created (it will take a few minutes), find the "Endpoint" and "Port" (the port should be 3306).
Save these for later use.

## 4. Create an SEC-API Account

Go to https://sec-api.io/
Click "Get Free API Key" and create an account.
Save your API key for later use.
NOTE The Free Version of this API key has a capacity of 100 queries. After that a new API key is required.

## 5. Set up your config file

Create a file titled .env. In this file, put your AWS login information and your API key to keep them secret.

## 6. Running the Stock Analysis Tool

To run the stock analysis tool, simply run the following command in the terminal: python3 main.py

This command will output each of the top 20 stocks, in the order of recommendation; the first stock is the most recommended of the group, whereas the last stock is the least recommended of the group.
