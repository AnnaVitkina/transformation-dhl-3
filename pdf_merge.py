# 2. Import modules
import os
import re
import sys
import shutil # Added for file operations
from pypdf import PdfWriter, PdfReader
from pathlib import Path
from google.colab import drive

# 3. Mount Google Drive
drive.mount('/content/drive')

# 4. Define helper function for natural sorting
def _natural_sort_key(path):
    """Sort key for natural order: page1, page2, page10, page20 (not page1, page10, page2, page20)."""
    name = path.name if isinstance(path, Path) else os.path.basename(path)
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r'(\d+)', name)]

# 5. Define merge_pdfs function
def merge_pdfs(input_files, output_file="merged_output.pdf"):
    """
    Merges multiple PDF files into a single PDF.

    Args:
        input_files: List of paths to PDF files to merge
        output_file: Path for the output merged PDF file (default: merged_output.pdf)

    Returns:
        bool: True if successful, False otherwise
    """
    if not input_files:
        print("Error: No PDF files provided.")
        return False

    # Validate input files
    valid_files = []
    for file_path in input_files:
        if not os.path.exists(file_path):
            print(f"Warning: File '{file_path}' not found. Skipping.")
            continue
        if not file_path.lower().endswith('.pdf'):
            print(f"Warning: '{file_path}' is not a PDF file. Skipping.")
            continue
        valid_files.append(file_path)

    if not valid_files:
        print("Error: No valid PDF files to merge.")
        return False

    print(f"\nMerging {len(valid_files)} PDF files...")

    try:
        # Create PDF writer object
        writer = PdfWriter()

        # Add each PDF file
        for idx, pdf_file in enumerate(valid_files, 1):
            print(f"  [{idx}/{len(valid_files)}] Adding: {os.path.basename(pdf_file)}")
            reader = PdfReader(pdf_file)
            for page in reader.pages:
                writer.add_page(page)

        # Write the merged PDF
        print(f"\nWriting merged PDF to: {output_file}")
        with open(output_file, 'wb') as output:
            writer.write(output)

        print(f"✓ Successfully merged {len(valid_files)} PDFs into '{output_file}'")
        return True

    except Exception as e:
        print(f"✗ Error merging PDFs: {e}")
        import traceback
        traceback.print_exc()
        return False

# 6. Define merge_pdfs_from_folder function (kept for completeness, but main logic uses merge_pdfs directly)
def merge_pdfs_from_folder(folder_path, output_file="merged_output.pdf", pattern="*.pdf"):
    """
    Merges all PDF files in a folder into a single PDF.

    Args:
        folder_path: Path to folder containing PDF files
        output_file: Path for the output merged PDF file
        pattern: File pattern to match (default: *.pdf)

    Returns:
        bool: True if successful, False otherwise
    """
    if not os.path.exists(folder_path):
        print(f"Error: Folder '{folder_path}' not found.")
        return False

    # Find all PDF files in the folder (natural sort: page1, page2, page10, page20)
    pdf_files = sorted(Path(folder_path).glob(pattern), key=_natural_sort_key)
    pdf_files = [str(f) for f in pdf_files if f.is_file()]

    if not pdf_files:
        print(f"Error: No PDF files found in '{folder_path}'")
        return False

    print(f"Found {len(pdf_files)} PDF files in folder: {folder_path}")
    return merge_pdfs(pdf_files, output_file)

# 7. Main execution logic for merging
print("=" * 60)
print("Consolidated PDF Merger Execution")
print("=" * 60)

# Hardcode your input folder path here
input_drive_folder = "/content/drive/Shareddrives/FA Ops Europe: Rate Maintenance Team /Documents/AI Adoption RMT/RMT/Input PDFs"
# Define the base output folder in Google Drive
output_base_folder ="/content/drive/Shareddrives/FA Ops Europe: Rate Maintenance Team /Documents/AI Adoption RMT/RMT/Output PDFs"

# Define the archive folder in Google Drive
archive_folder = "/content/drive/Shareddrives/FA Ops Europe: Rate Maintenance Team /Documents/AI Adoption RMT/RMT/Archive PDFs"

# Ensure the output and archive folders exist
os.makedirs(output_base_folder, exist_ok=True)
os.makedirs(archive_folder, exist_ok=True)

# Find all PDF files in the input folder
all_pdf_files_in_folder = sorted(Path(input_drive_folder).glob("*.pdf"), key=_natural_sort_key)
all_pdf_files_in_folder = [str(f) for f in all_pdf_files_in_folder if f.is_file()]

if not all_pdf_files_in_folder:
    print(f"Error: No PDF files found in '{input_drive_folder}'. Exiting.")
else:
    print(f"\nFound {len(all_pdf_files_in_folder)} PDF files in folder: {input_drive_folder}")

    # Ask user for merge type
    print("\nDo you want to:")
    print("  1. Merge ALL found PDF files")
    print("  2. Select SPECIFIC PDF files to merge")
    merge_choice = input("Enter your choice (1 or 2): ").strip()

    files_to_merge = [] # This will store the ORIGINAL paths of files chosen by the user

    if merge_choice == "1":
        files_to_merge = all_pdf_files_in_folder
    elif merge_choice == "2":
        print("\nAvailable PDF files:")
        for i, file_path in enumerate(all_pdf_files_in_folder):
            print(f"  {i+1}. {os.path.basename(file_path)}")

        while True:
            selection_input = input("\nEnter the numbers of the files you want to merge (e.g., '1,3,5' or 'all'): ").strip().lower()
            if selection_input == 'all':
                files_to_merge = all_pdf_files_in_folder
                break

            selected_indices = []
            try:
                if selection_input: # Only process if input is not empty
                    selected_indices = [int(x.strip()) - 1 for x in selection_input.split(',')]
            except ValueError:
                print("Invalid input. Please enter numbers separated by commas or 'all'.")
                continue

            valid_selection = True
            temp_files_to_merge_list = [] # Store selected files temporarily for validation
            for idx in selected_indices:
                if 0 <= idx < len(all_pdf_files_in_folder):
                    temp_files_to_merge_list.append(all_pdf_files_in_folder[idx])
                else:
                    print(f"Invalid file number: {idx+1}. Please enter valid numbers.")
                    valid_selection = False
                    break

            if valid_selection:
                if temp_files_to_merge_list:
                    files_to_merge = temp_files_to_merge_list
                    break
                else:
                    print("No files selected. Please enter valid numbers or 'all'.")
            else:
                print("Please try again.")

    else:
        print("Invalid choice. Merging all files by default.")
        files_to_merge = all_pdf_files_in_folder

    if files_to_merge:
        # Define a temporary folder for files being processed
        processing_temp_folder = os.path.join(output_base_folder, "temp_selected_pdfs")
        os.makedirs(processing_temp_folder, exist_ok=True)
        print(f"\nCopying selected PDFs to temporary processing folder: {processing_temp_folder}")

        processed_files_to_merge = []
        for original_file_path in files_to_merge:
            destination_file_path = os.path.join(processing_temp_folder, os.path.basename(original_file_path))
            shutil.copy2(original_file_path, destination_file_path) # Use copy2 to preserve metadata
            processed_files_to_merge.append(destination_file_path)
            print(f"  Copied: {os.path.basename(original_file_path)}")

        # Prompt the user for the output file name
        output_filename = input("\nEnter output file name (e.g., 'my_merged_doc.pdf', press Enter for 'merged_output.pdf'): ").strip()

        # Set a default if no name is provided
        if not output_filename:
            output_filename = "merged_output.pdf"

        # Construct the full output file path
        output_drive_file = os.path.join(output_base_folder, output_filename)

        # Call the merge function with the COPIED files
        if merge_pdfs(processed_files_to_merge, output_drive_file):
            # Move original files to archive after successful merge
            print(f"\nMoving original input files to archive folder: {archive_folder}")
            for original_file_path in files_to_merge:
                try:
                    shutil.move(original_file_path, os.path.join(archive_folder, os.path.basename(original_file_path)))
                    print(f"  Moved: {os.path.basename(original_file_path)}")
                except Exception as move_e:
                    print(f"  Error moving '{os.path.basename(original_file_path)}': {move_e}")

        # Clean up temporary files after merging
        print(f"\nCleaning up temporary processing folder: {processing_temp_folder}")
        shutil.rmtree(processing_temp_folder)
        print("Temporary files removed.")

    else:
        print("No files were selected for merging. Operation cancelled.")
