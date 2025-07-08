import sys
import pymupdf
import pdfquery


def main(args):
    doc = pymupdf.open("/temp/ICDC_Data_Model_2025-04.pdf")

    merged_pages = list()

    for pdf_page in doc:
        text = pdf_page.get_text().encode("utf-8")

        split_text = str(text).strip("\\n").strip("'b").split("\\n")

        end_index = split_text.index("CANINECOMMONS.CANCER.GOV/#/ICDC-DATA-MODEL")

        merged_pages.extend(split_text[0:end_index])

    curr_index = 0

    definition_dict = dict()

    while curr_index < len(merged_pages):
        if merged_pages[curr_index] == "PROPERTY":
            property_name = merged_pages[curr_index + 1].strip()
            if merged_pages[curr_index + 2] == "DESCRIPTION":
                property_description = merged_pages[curr_index + 3].strip()
            else:
                property_description = ""
            if property_name not in definition_dict:
                definition_dict[property_name] = property_description
            curr_index += 4
        else:
            curr_index += 1

    for name, definition in definition_dict.items():
        print(f"{name}: {definition}")


if __name__ == "__main__":
    main(sys.argv)
