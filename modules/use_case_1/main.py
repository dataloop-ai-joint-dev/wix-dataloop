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
            csv_path (str): Path to the CSV file
            dataset (dl.Dataset): Dataloop dataset object
        """

        import csv
        import os
        import time
        # CSV column indexes
        SITE_ID_INDEX = 0  # Site Identifier column
        COMP_ID_INDEX = 1  # Section Identifier column
        FULL_URL_INDEX = 2  # full_site_url column
        REVIEW_URL_INDEX = 3  # review_url column
        ORIG_IMG_INDEX = 4  # orig_section_image_url column
        GPT_URL_INDEX = 5  # gpt_s3_url column

        dataset = item.dataset
        csv_path = item.download()
        csv_filename = os.path.splitext(os.path.basename(csv_path))[0]
        folder_path = f"/{csv_filename}"
        dataset.items.make_dir(directory=folder_path)
        with open(csv_path, "r") as file:
            csv_reader = csv.reader(file)

            # Process each row
            for row_num, row in enumerate(csv_reader):
                try:
                    if row_num == 0:
                        print(f"Skipping header row: {row}")
                        continue

                    # Create prompt item
                    prompt_item = dl.PromptItem(name=f"{row[SITE_ID_INDEX]}")
                    item = dataset.items.upload(
                        prompt_item, remote_path=folder_path, overwrite=True
                    )
                    prompt_item = dl.PromptItem.from_item(item)

                    # Add context information
                    prompt_item.add(
                        message={
                            "role": "user",
                            "content": [
                                {
                                    "mimetype": dl.PromptType.TEXT,
                                    "value": "Please review the following website section layouts:\n\n"
                                             + f"[Original Site URL]({row[FULL_URL_INDEX]})  |  "
                                             + f"[Review Site URL]({row[REVIEW_URL_INDEX]})  |  "
                                             + f"[Original Section to Review]({row[ORIG_IMG_INDEX]})\n\n"
                                             + f"[Generated Section to Review]({row[GPT_URL_INDEX]})\n\n",
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
                                    "value": "Section 1",
                                }
                            ],
                        },
                        prompt_key="1",
                    )
                    time.sleep(1)
                    prompt_item.add(
                        message={
                            "role": "assistant",
                            "content": [
                                {
                                    "mimetype": dl.PromptType.TEXT,
                                    "value": "Section 2",
                                }
                            ],
                        },
                        prompt_key="1",
                    )
                    time.sleep(1)
                    prompt_item.add(
                        message={
                            "role": "assistant",
                            "content": [
                                {
                                    "mimetype": dl.PromptType.TEXT,
                                    "value": "Section 3",
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
