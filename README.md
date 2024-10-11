# 📈UWPD Analysis File Generator🚓

Welcome to the UWPD Analysis File Generator project! This application enables you to process and analyze Dispatched and Arrival CSV files for the UWPD. It leverages AWS services to automate data processing and provides an interactive web interface for ease of use.

## 🌟 Key Features
- Automated File Processing: Upload your CSV files, and the system automatically processes them to generate insightful analysis.
- AWS Lambda Integration: Utilizes AWS Lambda for serverless computation, ensuring scalability and reliability.
- Streamlit Web Interface: A user-friendly interface built with Streamlit for easy file uploads and downloads.
- Presigned URL Downloads: Securely download the generated analysis files via presigned URLs.

## 🏗️ Project Structure

### Backend 🖥️
The backend is composed of:

#### AWS Lambda Function:
##### Functionality:
- Fetches the most recent files from the arrival/ and dispatched/ folders in an S3 bucket.
- Processes the CSV files to compute time differences between dispatched and arrival times.
- Saves the output to the analysis/ folder in the same S3 bucket.
- Returns a presigned URL to download the analysis file.

##### Current Limitations:
- Bucket Name: Currently hardcoded. To enhance flexibility, consider passing the bucket name as an environment variable or function parameter.
- File Format: Also hardcoded. To support different file formats, modify the code to accept file format as a parameter.

### Frontend 💻
The frontend is a Streamlit application that:

#### Features:
- Allows users to upload the Dispatched and Arrival CSV files directly from their system.
- Upon uploading, it invokes the Lambda function to process the files.
- Displays a download button for users to retrieve the generated analysis file.

#### Current Implementation:
- Uses AWS CLI login credentials for S3 access. This requires AWS credentials to be configured on the machine running the frontend.

## 🚀 Getting Started
### Prerequisites
- Python 3.8+
- AWS Account with permissions for Lambda and S3.
- AWS CLI installed and configured (if not using the Lambda function for uploads).
- Necessary Python Packages: Install via pip install -r requirements.txt.

### Installation
Clone the Repository:

```bash
git clone https://github.com/ASUCICREPO/UWPD.git
cd UWPD
```

Install Dependencies:

```bash
pip install -r requirements.txt
```

## 🔧 Configuration

### Backend Setup

- S3 Bucket with three different folders "arrival/", "dispatched/" and "analysis/"
- Upload the Lambda function code (lambda_function.py) to AWS Lambda.
- Set Environment Variables:- BUCKET_NAME: Your S3 bucket name.
- Adjust IAM Permissions:- Ensure the Lambda function's execution role has permissions for S3 operations (s3:GetObject, s3:PutObject, s3:ListBucket).
- Enhancements: Make Bucket Name Flexible: Modify the function to accept the bucket name as an environment variable (if not already).
- Support Variable File Formats: Adjust the code to handle different file formats by parameterizing the file format.
- Functional URL: To simplify further, I created a functional URL with CORS enabled.

#### Frontend Setup

- Update Lambda API URL:- In app.py, replace 'YOUR_LAMBDA_API_URL' with your actual Lambda function's API Gateway URL.

```bash
LAMBDA_API_URL = 'https://your-api-id.execute-api.your-region.amazonaws.com/your-stage/your-resource'
```

- AWS Credentials:- Current Method: The frontend uses AWS CLI credentials to upload files to S3.
- Recommended Optimization:-Use Lambda for File Uploads: Implement a Lambda function to handle file uploads.

## 🎯 Run the Application
To run the frontend, use the following steps:

#### Navigate to the Frontend Folder:

```bash
cd frontend
```

#### Start the Streamlit App:

```bash
streamlit run app.py
```

#### Upload CSV Files:

- Upload the Dispatched and Arrival CSV files using the web interface.
- The Lambda function will process the files, and upon successful processing, a spinner will appear indicating that the backend has started working.
- Once the process completes, you will be provided with a Download Analysis File button. Clicking this button will allow you to download the analysis file using the presigned URL returned by the Lambda function.

## 🎨 Customization and UI Enhancements
- Streamlit UI Improvements:-Interactive Elements: Clear instructions, buttons with icons, and progress spinners for better user experience.
- Error Handling: User-friendly messages with appropriate emojis.

Happy Analyzing! 📈🚀😊