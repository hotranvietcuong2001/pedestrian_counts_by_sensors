
from airflow.exceptions import AirflowSkipException

def map_files_for_upload(filename:str):
    """
    If the file extension is parquet, return the filename, otherwise skip the upload
    Using this function for dynamic task on Airflow
    :param filename: the name of the file to be uploaded
    :type filename: str
    :return: The filename is being returned if the file extension is parquet.
    """
    if filename.rsplit(".", 1)[-1] in ("parquet"):
        return filename
    raise AirflowSkipException(f"Skip upload: {filename}")