import dtlpy as dl
import logging
import csv
import os

logger = logging.getLogger(name="dtlpy")


class ServiceRunner(dl.BaseServiceRunner):

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
        import time

        # CSV column indexes (based on the provided CSV structure)
        SITE_ID_INDEX = 0  # Site Identifier column
        URL_INDEX = 1  # url column
        TYPES_INDEX = 2  # GT_types column
        CATEGORIES_INDEX = 3  # GT_categories column

        dataset = item.dataset
        csv_path = item.download()
        csv_filename = os.path.splitext(os.path.basename(csv_path))[0]
        folder_path = f'/{csv_filename}'
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
                    prompt_item = dl.PromptItem(name=row[SITE_ID_INDEX])
                    item = dataset.items.upload(prompt_item, remote_path=folder_path, overwrite=True)
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
                    time.sleep(0.5)
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
                    time.sleep(0.5)
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
                                time.sleep(0.5)

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
                    time.sleep(0.5)

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
                                time.sleep(0.5)

                    # Add metadata
                    metadata = {"msid": row[SITE_ID_INDEX]}
                    prompt_item._item.metadata["user"] = metadata
                    prompt_item._item.update(True)

                    print(f"Uploaded item {item.id} with metadata {metadata}")

                except Exception as e:
                    print(f"Error processing row {row_num}: {str(e)}")
                    continue

        os.remove(csv_path)

    @staticmethod
    def process_dataset(dataset: dl.Dataset, query=None, progress=None):
        if query:
            filters = dl.Filters(custom_filter=query)
        else:
            filters = dl.Filters()
        filters.add(field='metadata.system.mimetype', values="text/csv", method=dl.FILTERS_METHOD_OR)
        filters.sort_by(field='id', value=dl.FiltersOrderByDirection.ASCENDING)
        items = dataset.items.list(filters=filters).all()
        for item in items:
            ServiceRunner.process_csv_use_case_2(item)
        
        return dataset
