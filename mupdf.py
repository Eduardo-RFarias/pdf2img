import fitz
import time
import os
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, as_completed
from PIL import Image
import base64
import io


def process_page(pdf_path, page_num, output_dir, dpi=300, img_format="webp", quality=80):
    """Process a single PDF page and save as image using PIL"""
    page_start_time = time.time()

    # Set DPI
    zoom_x = dpi / 72
    zoom_y = dpi / 72
    matrix = fitz.Matrix(zoom_x, zoom_y)

    # Open the PDF and get the specific page
    doc = fitz.open(pdf_path)
    page = doc.load_page(page_num)
    pix = page.get_pixmap(matrix=matrix)

    # Convert PyMuPDF pixmap to PIL Image
    # This is faster than using pix.save() for some formats
    img_data = pix.samples
    img = Image.frombytes("RGB", [pix.width, pix.height], img_data)

    # Save the image with PIL - WebP is generally faster to encode than JPEG
    # while maintaining good quality/size ratio
    file_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_image = os.path.join(output_dir, f"{file_name}_{page_num + 1}.{img_format}")

    if img_format.lower() == "webp":
        img.save(output_image, format="WEBP", quality=quality, method=0)  # method=0 is fastest
    elif img_format.lower() == "jpeg" or img_format.lower() == "jpg":
        img.save(output_image, format="JPEG", quality=quality, optimize=False)
    elif img_format.lower() == "png":
        img.save(output_image, format="PNG", optimize=False, compress_level=1)  # Lower compress_level is faster
    elif img_format.lower() == "base64":
        # Create a buffer to hold the image data
        buffer = io.BytesIO()
        # Save the image to the buffer in WEBP format
        img.save(buffer, format="WEBP", quality=quality, method=0)
        # Get the base64 encoded string
        b64_str = base64.b64encode(buffer.getvalue())
        # Create the output filename with b64 extension
        b64_output = os.path.splitext(output_image)[0] + ".b64"
        # Write the base64 string to a file in binary mode for better performance
        with open(b64_output, "wb") as f:
            f.write(b64_str)

        # Free memory explicitly
        buffer.close()
    else:
        # Default to WebP if format not recognized
        img.save(output_image, format="WEBP", quality=quality, method=0)

    # Close the document
    doc.close()

    page_end_time = time.time()
    page_duration = page_end_time - page_start_time

    return page_num, page_duration


def pdf_to_jpeg(pdf_path, output_dir="output_mupdf", dpi=300, parallel=False, max_workers=None, img_format="webp", quality=80):
    """
    Convert each page of a PDF to an image using PyMuPDF and PIL.

    Args:
        pdf_path (str): Path to the PDF file
        output_dir (str): Directory to save the images (will be created if it doesn't exist)
        dpi (int): Resolution for the output images
        parallel (bool): Whether to process pages in parallel
        max_workers (int): Maximum number of worker processes to use (None = auto)
        img_format (str): Image format to save as (webp, jpeg, png, base64)
        quality (int): Image quality (1-100, higher is better quality but larger file)
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Open the PDF file to get page count
    doc = fitz.open(pdf_path)
    page_count = len(doc)
    doc.close()

    print(f"PyMuPDF: Converting PDF with {page_count} pages" + f" using {'parallel' if parallel else 'sequential'} processing...")

    # Start total time measurement
    total_start_time = time.time()

    if parallel:
        # Process pages in parallel
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = [executor.submit(process_page, pdf_path, i, output_dir, dpi, img_format, quality) for i in range(page_count)]

            # Process results as they complete
            for future in as_completed(futures):
                page_num, page_duration = future.result()
                print(f"PyMuPDF: Page {page_num + 1}/{page_count} conversion took {page_duration:.2f} seconds")
    else:
        # Process pages sequentially
        for page_num in range(page_count):
            page_num, page_duration = process_page(pdf_path, page_num, output_dir, dpi, img_format, quality)
            print(f"PyMuPDF: Page {page_num + 1}/{page_count} conversion took {page_duration:.2f} seconds")

    # End total time measurement
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time

    print(f"PyMuPDF: Total conversion time: {total_duration:.2f} seconds")


if __name__ == "__main__":
    # Set up argument parser
    parser = ArgumentParser(description="Convert PDF to images using PyMuPDF and PIL")
    parser.add_argument("pdf_path", help="Path to the PDF file to convert")
    parser.add_argument("--output-dir", default="output", help="Directory to save the output images")
    parser.add_argument("--dpi", type=int, default=300, help="Resolution for the output images")
    parser.add_argument("--parallel", action="store_true", help="Process pages in parallel")
    parser.add_argument("--max-workers", type=int, default=None, help="Maximum number of worker processes")
    parser.add_argument("--format", default="webp", choices=["webp", "jpeg", "jpg", "png", "base64"], help="Image format to save as")
    parser.add_argument("--quality", type=int, default=80, help="Image quality (1-100)")

    args = parser.parse_args()

    # Call the conversion function with the provided arguments
    pdf_to_jpeg(
        args.pdf_path,
        output_dir=args.output_dir,
        dpi=args.dpi,
        parallel=args.parallel,
        max_workers=args.max_workers,
        img_format=args.format,
        quality=args.quality,
    )
