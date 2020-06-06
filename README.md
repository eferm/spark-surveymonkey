# spark-surveymonkey
Convert SurveyMonkey respones in JSON to a PySpark DataFrame

## Usage
```python
from spark_surveymonkey import transform_survey

df = transform_survey(spark_session, 'path/to/files/', 'path/to/details.json')
```

Function signature:
```python
def transform_survey(spark_session: SparkSession,
                     survey_responses: str,
                     survey_details: Union[str, Dict[str, str]]) -> DataFrame:
    """Convert SurveyMonkey responses in JSON to a PySpark `DataFrame`.

    Args:
        spark_session: A `SparkSession` object.
        survey_responses: Path to directory containing JSON files.
            Files retrieved from API endpoint `surveys/{survey_id}/responses/bulk`.
        survey_details: Path to JSON file, or `dict` of JSON file.
            File retrieved from API endpoint `surveys/{survey_id}/details`.

    Returns:
        `DataFrame`: Survey responses with 1 row per response.
    """
```
