import streamlit as st
import boto3
from botocore.exceptions import NoCredentialsError
import requests

# AWS S3 Configuration
S3_BUCKET = 'researchoreinted'

# Initialize S3 Client
s3 = boto3.client('s3')

# Replace this with your actual Lambda API URL
LAMBDA_API_URL = 'https://3uzxrflxzcamcdv3lv6civxoo40regwk.lambda-url.us-west-2.on.aws/'

def upload_to_s3(file, folder):
    try:
        s3.upload_fileobj(file, S3_BUCKET, folder + file.name)
        return True
    except FileNotFoundError:
        st.error("🚫 The file was not found.")
        return False
    except NoCredentialsError:
        st.error("🔑 AWS credentials not available.")
        return False
    except Exception as e:
        st.error(f"⚠️ An error occurred: {e}")
        return False

def main():
    st.set_page_config(page_title="UWPD Analysis File Generator", page_icon="📊")
    st.title('📊 UWPD Analysis File Generator')

    st.write("Upload your **Dispatched** and **Arrival** CSV files to generate the analysis report.")

    dispatched_file = st.file_uploader("📄 Upload Dispatched File", type=["csv"], key="dispatched")
    arrival_file = st.file_uploader("📄 Upload Arrival File", type=["csv"], key="arrival")

    if st.button('🚀 Upload and Generate Analysis'):
        if dispatched_file and arrival_file:
            with st.spinner('Uploading files to S3... ⏳'):
                # Upload dispatched file
                dispatched_upload = upload_to_s3(dispatched_file, 'dispatched/')
                # Upload arrival file
                arrival_upload = upload_to_s3(arrival_file, 'arrival/')

            if dispatched_upload and arrival_upload:
                st.success("✅ Files uploaded successfully!")

                with st.spinner('Processing files and generating analysis... 🛠️'):
                    # Call the Lambda function via API Gateway
                    try:
                        response = requests.get(LAMBDA_API_URL)
                        if response.status_code == 200:
                            data = response.json()
                            if 'presigned_url' in data:
                                presigned_url = data['presigned_url']
                                st.success("🎉 Analysis completed!")

                                # Provide download button
                                st.markdown("---")
                                st.header('📥 Download Analysis File')
                                st.write("Click the button below to download the analysis file.")

                                # Fetch the file content
                                file_response = requests.get(presigned_url)
                                if file_response.status_code == 200:
                                    st.download_button(
                                        label="⬇️ Download Analysis File",
                                        data=file_response.content,
                                        file_name='analysis.csv',
                                        mime='text/csv'
                                    )
                                else:
                                    st.error("⚠️ Failed to download the analysis file.")
                            else:
                                st.error("⚠️ Error: Presigned URL not found in the response.")
                        else:
                            st.error(f"⚠️ Error from Lambda function: {response.status_code} {response.text}")
                    except Exception as e:
                        st.error(f"⚠️ Error calling Lambda function: {e}")
            else:
                st.error("❌ Failed to upload files. Please try again.")
        else:
            st.warning("⚠️ Please upload both files before proceeding.")

if __name__ == '__main__':
    main()