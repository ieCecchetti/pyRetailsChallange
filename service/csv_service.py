import pandas as pd
import logging


class CsvService:
    """CSV Service class containing all methods for usage of csv"""
    logger = logging.getLogger("my logger")

    def __init__(self):
        raise RuntimeError("Constructor cannot be called.")

    @staticmethod
    def open_excel(csv_path: str):
        """
        Open a csv file at path csv_name
        returns: pandas interface of the xlsx
        """
        logger = logging.getLogger("my logger")
        if csv_path is None:
            logger.error("Error. path cant be null.")
            return None
        if csv_path is "":
            logger.error("Error. path cant be Empty.")
            return None
        logger.info("Opening file at %s." % csv_path)
        try:
            df_retail = pd.read_excel(
                csv_path,
                engine='openpyxl'
            )
            logger.info("File %s loaded correctly." % csv_path)
            return df_retail
        except FileNotFoundError:
            logger.error("[Error] File at path [%s] not found." % csv_path)
            return None
