import dtlpy as dl
import logging

logger = logging.getLogger(name="dtlpy")


class ServiceRunner(dl.BaseServiceRunner):
    """
    Package runner class

    """

    def __init__(self):
        """
        Init package attributes here

        :return:
        """

    @staticmethod
    def process_csv_use_case_2(item: dl.Item):
        """
        Upload prompts from CSV file to Dataloop dataset using CSV reader

        Args:
            csv_path (str): Path to the CSV file
            dataset (dl.Dataset): Dataloop dataset object
        """

        import csv
        import os

        # CSV column indexes (based on the provided CSV structure)
        SITE_ID_INDEX = 0  # Site Identifier column
        URL_INDEX = 1  # url column
        TYPES_INDEX = 2  # GT_types column
        CATEGORIES_INDEX = 3  # GT_categories column

        dataset = item.dataset
        csv_path = item.download()
        with open(csv_path, "r") as file:
            csv_reader = csv.reader(file)

            # Process each row
            for row_num, row in enumerate(csv_reader):
                try:
                    # Skip empty rows or rows with missing essential data
                    if len(row) < 5 or not row[SITE_ID_INDEX] or not row[URL_INDEX]:
                        continue
                    if row_num == 0:
                        print(f"Skipping header row: {row}")
                        continue

                    # Create prompt item
                    prompt_item = dl.PromptItem(name=row[SITE_ID_INDEX])
                    item = dataset.items.upload(prompt_item, overwrite=True)
                    prompt_item = dl.PromptItem.from_item(item)

                    # Add URL prompt
                    prompt_item.add(
                        message={
                            "role": "user",
                            "content": [
                                {
                                    "mimetype": dl.PromptType.TEXT,
                                    "value": f"[Site URL]({row[URL_INDEX]})",
                                }
                            ],
                        },
                        prompt_key="1",
                    )

                    # Add type selection prompt
                    prompt_item.add(
                        message={
                            "role": "user",
                            "content": [
                                {
                                    "mimetype": dl.PromptType.TEXT,
                                    "value": "Please select the leading type for this site:",
                                }
                            ],
                        },
                        prompt_key="2",
                    )

                    # Add type options
                    if row[TYPES_INDEX]:
                        for type_option in row[TYPES_INDEX].split("\n"):
                            if type_option:  # Skip empty options
                                prompt_item.add(
                                    message={
                                        "role": "assistant",
                                        "content": [
                                            {
                                                "mimetype": dl.PromptType.TEXT,
                                                "value": type_option,
                                            }
                                        ],
                                    },
                                    prompt_key="2",
                                )

                    # Add category selection prompt
                    prompt_item.add(
                        message={
                            "role": "user",
                            "content": [
                                {
                                    "mimetype": dl.PromptType.TEXT,
                                    "value": "Please select the leading category for this site:",
                                }
                            ],
                        },
                        prompt_key="3",
                    )

                    # Add category options
                    if row[CATEGORIES_INDEX]:
                        for category_option in row[CATEGORIES_INDEX].split("\n"):
                            if category_option:  # Skip empty options
                                prompt_item.add(
                                    message={
                                        "role": "assistant",
                                        "content": [
                                            {
                                                "mimetype": dl.PromptType.TEXT,
                                                "value": category_option,
                                            }
                                        ],
                                    },
                                    prompt_key="3",
                                )

                    # Add metadata
                    metadata = {"msid": row[SITE_ID_INDEX]}
                    prompt_item._item.metadata["user"] = metadata
                    prompt_item._item.update(True)

                    print(f"Uploaded item {item.id} with metadata {metadata}")

                except Exception as e:
                    print(f"Error processing row {row_num}: {str(e)}")
                    continue

        os.remove(csv_path)