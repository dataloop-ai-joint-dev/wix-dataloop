import dtlpy as dl
import logging
import csv
import os

logger = logging.getLogger(name="dtlpy")


class ServiceRunner(dl.BaseServiceRunner):

    @staticmethod
    def process_csv_use_case_1(item: dl.Item):
        """
        Upload prompts for layout validation from CSV file to Dataloop dataset

        Args:
            item (dl.Item): Dataloop Item
        """

        # CSV column indexes
        SITE_ID_INDEX = 1  # Site Identifier column
        COMP_ID_INDEX = 2  # Section Identifier column
        FULL_URL_INDEX = 3  # full_site_url column
        REVIEW_URL_INDEX = 4  # review_url column
        ORIG_IMG_INDEX = 5  # orig_section_image_url column
        GPT_URL_INDEX = 6  # gpt_s3_url column

        dataset = item.dataset
        csv_path = item.download()
        with open(csv_path, "r") as file:
            csv_reader = csv.reader(file)
            # Skip header and explanation rows
            next(csv_reader)

            # Process each row
            for row_num, row in enumerate(csv_reader, start=1):
                try:
                    # Skip empty rows or rows with missing essential data
                    if len(row) < 7 or not row[SITE_ID_INDEX] or not row[COMP_ID_INDEX]:
                        continue

                    # Create prompt item
                    prompt_item = dl.PromptItem(name=f"{row[SITE_ID_INDEX]}")
                    item = dataset.items.upload(prompt_item, overwrite=True)

                    # Initialize from item
                    prompt_item = dl.PromptItem.from_item(item)

                    # Add context information
                    prompt_item.add(
                        message={
                            "role": "user",
                            "content": [
                                {
                                    "mimetype": dl.PromptType.TEXT,
                                    "value": "Please review the following website section layouts:\n\n"
                                             + f"[Original Site]({row[FULL_URL_INDEX]})\n\n"
                                             + f"[Original Section Image]({row[ORIG_IMG_INDEX]})\n",
                                }
                            ],
                        },
                        prompt_key="1",
                    )

                    prompt_item.add(
                        message={
                            "role": "assistant",
                            "content": [
                                {
                                    "mimetype": dl.PromptType.TEXT,
                                    "value": f"[Generated Layouts]({row[GPT_URL_INDEX]})\n",
                                }
                            ],
                        },
                        prompt_key="1",
                    )
                    prompt_item.add(
                        message={
                            "role": "assistant",
                            "content": [
                                {
                                    "mimetype": dl.PromptType.TEXT,
                                    "value": f"[Review URL]({row[REVIEW_URL_INDEX]})",
                                }
                            ],
                        },
                        prompt_key="1",
                    )

                    metadata = {
                        "site_id": row[SITE_ID_INDEX],
                        "component_id": row[COMP_ID_INDEX],
                    }
                    prompt_item._item.metadata["user"] = metadata
                    prompt_item._item.update(True)

                    print(f"Uploaded item {item.id} with metadata {metadata}")

                except Exception as e:
                    print(f"Error processing row {row_num}: {str(e)}")
                    continue

        os.remove(csv_path)
