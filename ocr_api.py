from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import pandas as pd
import pytesseract
from pdf2image import convert_from_path
import configparser
from PyPDF2 import PdfReader, PdfWriter
import time
from PIL import Image
import boto3


app = Flask(__name__)
CORS(app)

def parse_config_file(config_file):
    # Parse the configuration file and extract settings
    config = configparser.ConfigParser()
    config.read(config_file)
    input_dir = config.get('Files', 'INPUT')
    output_dir = config.get('Files', 'OUTPUT')
    ocr_locations = {}
    for key, value in config.items('OCR'):
        # print(key, value)
        if key.startswith('text'):
            # Map the keywords to the respective coordinates
            ocr_locations[value.strip('"').upper()] = tuple(map(int, config.get('OCR', f'loc{key[4:]}').split(',')))
            # print(ocr_locations)
    return input_dir, output_dir, ocr_locations

def process_text_ocr(page):
    # Extract text from each page of the PDF file
    text = ''
    # print(loc)
    # page = page.crop(loc)
    text  = pytesseract.image_to_string(page)
    # print('------------------------')
    # print(text)
    return text

def seperate_sub_documents(pdf_file, output_path, pdf_pages, ocr_locations):
    # Upload the original PDF file to S3
    s3_client = boto3.client('s3')
    s3_client.upload_file(pdf_file, 'combinedpdfsbucket', f'{os.path.basename(pdf_file)}')
    # Identify sub-documents based on specified keywords and locations
    curr_subdoc_type = ''
    curr_start_page = 0
    curr_end_page = 0
    for page_num, page in enumerate(pdf_pages):
        text = process_text_ocr(page).upper()
        # Check if any keyword is present in the text
        for word, loc in ocr_locations.items():
            # print(word, loc)
            # text = process_text_ocr(page, loc).upper()
            if word in text and curr_subdoc_type == '':
                # print(1)
                curr_subdoc_type = word
                curr_start_page = page_num
                break
            # Start of a new sub-document
            elif word in text and word != curr_subdoc_type:
                curr_end_page = page_num - 1
                create_sub_documents(pdf_file, output_path, curr_subdoc_type, curr_start_page, curr_end_page)
                curr_subdoc_type = word
                curr_start_page = page_num

    if curr_subdoc_type != '':
        curr_end_page = page_num
        create_sub_documents(pdf_file, output_path, curr_subdoc_type, curr_start_page, curr_end_page)

   

def create_sub_documents(pdf_file, output_path, curr_subdoc_type, curr_start_page, curr_end_page):
    # Save each sub-document as a separate PDF
    # print(curr_start_page, curr_end_page)
    output_file_path = os.path.join(output_path, f'{os.path.basename(pdf_file).split(".")[0]}_{curr_subdoc_type}.pdf')
    pdf_writer = PdfWriter()
    pdf_reader = PdfReader(open(pdf_file, 'rb'))
    for page_num in range(curr_start_page, curr_end_page + 1):
        pdf_writer.add_page(pdf_reader.pages[page_num])
    with open(output_file_path, 'wb') as output_file:
        pdf_writer.write(output_file)
    #Upload the subdocuments to S3
    s3_client = boto3.client('s3')
    with open(output_file_path, 'rb') as output_file:
        s3_client.upload_file(output_file.name, 'combinedpdfsbucket', f'{os.path.basename(output_file.name)}')

def seperate_combined_pdfs(config_file):
    
    input_path, output_path, ocr_locations = parse_config_file(config_file)
    # print(ocr_locations)
    
    for file_name in os.listdir(input_path):
        if file_name.endswith('.pdf'):
            pdf_file = os.path.join(input_path, file_name)
            pdf_pages = convert_from_path(pdf_file)           # Convert PDF pages into images
            # print(pages)
            # Split the pages into sub-documents
            seperate_sub_documents(pdf_file, output_path, pdf_pages, ocr_locations)  
            # text = extract_text_from_pdf(pdf_pages)
            # print(text)

@app.route('/get_pdfs', methods=['GET'])
def get_pdfs():
    # List objects in the bucket
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket='combinedpdfsbucket')
    print(response)
    
    # Initialize a list to store the PDFs information
    pdfs_info = []
    
    if 'Contents' in response:
        print(f"\nObjects in bucket 'combinedpdfsbucket':")
        for obj in response['Contents']:
            key = obj['Key']
            # Check if the object is a splitted PDF based on the original PDF name
            if key.startswith('123456_'):
                # Extract PDF name, last modified, and object URL
                pdf_name = key.split('_')[0] + '.pdf'
                last_modified = obj['LastModified']
                object_url = f"https://combinedpdfsbucket.s3.amazonaws.com/{key}"
                # Append the PDF information to the list
                pdfs_info.append({'name': pdf_name, 'last_modified': last_modified, 'object_url': object_url})
    
    else:
        print(f"\nBucket 'combinedpdfsbucket' does not contain any objects.")
    
    print(jsonify(pdfs_info))
    # Return the list of PDFs information as JSON
    return jsonify(pdfs_info)

@app.route('/process_pdfs', methods=['GET'])
def process_pdfs():
    start_time = time.time()
    config_file = 'Sample.cfg'  
    seperate_combined_pdfs(config_file)
    end_time = time.time()
    execution_time = end_time - start_time
    return jsonify({"message": "PDFs processed successfully", "execution_time": execution_time})

if __name__ == "__main__":
    app.run(debug=True)  